/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.exec.operator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryManager;
import io.dingodb.common.memory.ObjectSizeUtils;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.profile.Profile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.memory.MemoryRevoker;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.store.api.transaction.exception.LockWaitException;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public class HashJoinOperator extends SoleOutOperator implements MemoryRevoker {
    public static final HashJoinOperator INSTANCE = new HashJoinOperator();

    private HashJoinOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        HashJoinParam param = vertex.getParam();
        try {
            TupleMapping leftMapping = param.getLeftMapping();
            TupleMapping rightMapping = param.getRightMapping();
            int leftLength = param.getLeftLength();
            int rightLength = param.getRightLength();
            boolean leftRequired = param.isLeftRequired();
            int pin = context.getPin();
            param.setContext(context);
            if (pin == 0) { // left (probe side)
                waitRightFinFlag(param, vertex);
                OperatorProfile profile = param.getProfile("hashJoin");
                long start = System.currentTimeMillis();
                TupleKey leftKey = HashJoinParam.rtrimTupleKey(new TupleKey(leftMapping.revMap(tuple)));
                // Null-key handling: emit immediately for OUTER joins, skip for INNER
                if (HashJoinParam.containsNull(leftKey)) {
                    if ("inner".equalsIgnoreCase(param.getJoinType())
                        || "right".equalsIgnoreCase(param.getJoinType())) {
                        return true;
                    } else if ("left".equalsIgnoreCase(param.getJoinType())
                        || "full".equalsIgnoreCase(param.getJoinType())) {
                        Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                        Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                        return pushToNext(param, edge, context, newTuple);
                    }
                }
                // Spill routing: if this partition was spilled, write left tuple to disk
                if (param.isSpillEnabled() && param.hasSpilledPartitions()) {
                    int partition = param.partitionOf(leftKey);
                    if (param.isPartitionSpilled(partition)) {
                        try {
                            param.spillLeftTuple(partition, tuple);
                        } catch (IOException e) {
                            throw new RuntimeException(
                                "Failed to spill left tuple for partition " + partition, e);
                        }
                        return true;
                    }
                }
                // In-memory probe (original logic for non-spilled partitions)
                boolean isEmpty = isEmpty(leftKey, param);
                if (isEmpty && ("inner".equalsIgnoreCase(param.getJoinType())
                    || "right".equalsIgnoreCase(param.getJoinType()))) {
                    profile.opTime(start);
                    return true;
                }
                if (isEmpty && "left".equalsIgnoreCase(param.getJoinType())) {
                    Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                    Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                    profile.opTime(start);
                    return pushToNext(param, edge, context, newTuple);
                }
                List<TupleWithJoinFlag> rightList = param.getHashMap().get(leftKey);
                if (rightList != null) {
                    for (TupleWithJoinFlag t : rightList) {
                        Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                        System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                        t.setJoined(true);
                        profile.opTime(start);
                        if (!pushToNext(param, edge, context, newTuple)) {
                            return false;
                        }
                    }
                } else if (leftRequired) {
                    Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                    Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                    profile.opTime(start);
                    return pushToNext(param, edge, context, newTuple);
                }
            } else if (pin == 1) { // right (build side)
                if (param.getSize() != null && param.getSize().get() > ScopeVariables.joinSpillSize()
                    && !param.getExecutionContext().isInnerSql()) {
                    param.getMemoryAllocatorCtx().allocateRevocableMemory(param.getSize().get());
                    param.getSize().set(0);
                }
                OperatorProfile profile = param.getProfile("hashJoin");
                long start = System.currentTimeMillis();
                TupleKey rightKey = HashJoinParam.rtrimTupleKey(new TupleKey(rightMapping.revMap(tuple)));
                // If this partition is already spilled, write directly to disk
                if (param.isSpillEnabled() && param.hasSpilledPartitions()
                    && param.isPartitionSpilled(param.partitionOf(rightKey))) {
                    try {
                        param.spillRightTuple(param.partitionOf(rightKey), tuple);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to spill right tuple to disk", e);
                    }
                    profile.cacheOpTime(start);
                    return true;
                }
                if (HashJoinParam.containsNull(rightKey)) {
                    if ("inner".equalsIgnoreCase(param.getJoinType())
                        || "left".equalsIgnoreCase(param.getJoinType())) {
                        return true;
                    } else if ("right".equalsIgnoreCase(param.getJoinType())
                        || "full".equalsIgnoreCase(param.getJoinType())) {
                        List<TupleWithJoinFlag> list = param.getHashMap()
                            .computeIfAbsent(rightKey, k -> Collections.synchronizedList(new LinkedList<>()));
                        long size = ObjectSizeUtils.calculateSize(tuple);
                        param.incMemSize(size);
                        list.add(new TupleWithJoinFlag(tuple));
                        param.incrementTupleCount();
                    }
                } else {
                    if (isEmpty(rightKey, param) && "inner".equalsIgnoreCase(param.getJoinType())) {
                        profile.cacheOpTime(start);
                        return true;
                    }
                    long size = ObjectSizeUtils.calculateSize(tuple);
                    param.getSize().addAndGet(size);
                    // Track revocable memory
                    if (param.getMemoryAllocatorCtx() != null) {
                        param.getMemoryAllocatorCtx().allocateRevocableMemory(size);
                    }
                    List<TupleWithJoinFlag> list = param.getHashMap()
                        .computeIfAbsent(rightKey, k -> Collections.synchronizedList(new LinkedList<>()));
                    list.add(new TupleWithJoinFlag(tuple));
                    param.incrementTupleCount();
                }
                // Check spill condition: threshold or memory revoking requested
                boolean shouldSpill = param.isSpillEnabled()
                    && (param.getTupleCount() >= SpillManager.DEFAULT_SPILL_THRESHOLD
                    || (param.getMemoryAllocatorCtx() != null
                    && param.getMemoryAllocatorCtx().isMemoryRevokingRequested()));
                if (shouldSpill) {
                    try {
                        param.spillPartitions();
                        if (param.getMemoryAllocatorCtx() != null) {
                            param.getMemoryAllocatorCtx().releaseRevocableMemory(
                                param.getMemoryAllocatorCtx().getRevocableAllocated(), true);
                            param.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to spill hash join build side", e);
                    }
                }
                profile.cacheOpTime(start);
            }
            return true;
        } finally {
            checkStatusAndInterrupt(param, vertex);
        }
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        HashJoinParam param = vertex.getParam();
        checkStatusAndInterrupt(param, vertex);
        if (fin instanceof FinWithException) {
            param.interrupt();
            edge.fin(fin);
            return;
        }
        if (!param.getHashMap().isEmpty() && !param.getExecutionContext().isInnerSql() && pin == 1) {
            long allocated = param.getMemoryAllocatorCtx().getAllAllocated();
            if (allocated > 0) {
                long size = param.getHashMap().size();
                long globalUsage = MemoryManager.getInstance().getGlobalMemoryPool().getMemoryUsage();
                long queryUsage = param.getQueryMemoryPool().getMemoryUsage();
                LogUtils.info(log, "hashMap size:{}, joinOperatorUsage:{}, queryUsage:{}, globalUsage:{}",
                    size, allocated, queryUsage, globalUsage);
            }
        }
        boolean rightRequired = param.isRightRequired();
        int leftLength = param.getLeftLength();
        int rightLength = param.getRightLength();
        if (pin == 0) { // left fin
            if (rightRequired) {
                // should wait in case of no data push to left.
                waitRightFinFlag(param, vertex);
                // Emit unmatched right tuples from in-memory hashMap
                outer:
                for (List<TupleWithJoinFlag> tList : param.getHashMap().values()) {
                    for (TupleWithJoinFlag t : tList) {
                        if (!t.isJoined()) {
                            Object[] newTuple = new Object[leftLength + rightLength];
                            Arrays.fill(newTuple, 0, leftLength, null);
                            System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                            if (!pushToNext(param, edge, param.getContext(), newTuple)) {
                                break outer;
                            }
                        }
                    }
                }
            }
            // Process spilled partitions
            if (param.hasSpilledPartitions()) {
                processSpilledPartitions(param, edge, leftLength, rightLength);
            }
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                param.setProfileLeft(finWithProfiles.getProfile());
                Profile profile = param.getProfile();
                if (profile == null) {
                    profile = param.getProfile("hashJoin");
                }
                profile.getChildren().add(param.profileLeft);
                profile.getChildren().add(param.profileRight);
                profile.end();
                finWithProfiles.setProfile(profile);
            }
            edge.fin(fin);
            // Reset
            param.clear();
            checkStatusAndInterrupt(param, vertex);
        } else if (pin == 1) { // right fin
            // Finalize all right spill files before signaling completion
            if (param.isSpillEnabled()) {
                try {
                    param.finishRightSpillFiles();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to finalize right spill files", e);
                }
            }
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                param.setProfileRight(finWithProfiles.getProfile());
            }
            param.setRightFinFlag(true);
            param.getFuture().complete(null);
        }
    }

    // -------------------------------------------------------------------------
    // Spilled partition processing
    // -------------------------------------------------------------------------

    /**
     * Processes all spilled partitions: for each partition, loads the right spill file
     * into a temporary hash map, streams the left spill file and probes against it,
     * then emits unmatched right tuples for OUTER joins.
     */
    private static void processSpilledPartitions(
        HashJoinParam param, Edge edge, int leftLength, int rightLength
    ) {
        boolean leftRequired = param.isLeftRequired();
        boolean rightRequired = param.isRightRequired();
        TupleMapping leftMapping = param.getLeftMapping();
        TupleMapping rightMapping = param.getRightMapping();

        for (int p = 0; p < HashJoinParam.NUM_PARTITIONS; p++) {
            if (!param.isPartitionSpilled(p)) {
                continue;
            }
            try {
                // Finish writing left spill file (may still be in write mode)
                TupleSpillFile leftSf = param.getLeftSpillFiles()[p];
                TupleSpillFile rightSf = param.getRightSpillFiles()[p];
                if (leftSf != null) {
                    leftSf.finishWrite();
                }

                // Step 1: Load right spill file into temporary hash map
                if (rightSf != null && rightSf.getTupleCount() > SpillManager.DEFAULT_SPILL_THRESHOLD) {
                    LogUtils.warn(log, "Spilled partition {} has {} tuples, "
                        + "may cause memory pressure during rebuild", p, rightSf.getTupleCount());
                }
                HashMap<TupleKey, List<TupleWithJoinFlag>> tempMap = new HashMap<>();
                if (rightSf != null) {
                    Iterator<Object[]> rightIter = rightSf.iterator();
                    while (rightIter.hasNext()) {
                        Object[] rightTuple = rightIter.next();
                        TupleKey rightKey = HashJoinParam.rtrimTupleKey(
                            new TupleKey(rightMapping.revMap(rightTuple)));
                        List<TupleWithJoinFlag> list = tempMap.computeIfAbsent(
                            rightKey, k -> new ArrayList<>());
                        list.add(new TupleWithJoinFlag(rightTuple));
                    }
                }

                // Step 2: Stream left spill file and probe against temp map
                if (leftSf != null) {
                    Iterator<Object[]> leftIter = leftSf.iterator();
                    while (leftIter.hasNext()) {
                        Object[] leftTuple = leftIter.next();
                        TupleKey leftKey = HashJoinParam.rtrimTupleKey(
                            new TupleKey(leftMapping.revMap(leftTuple)));

                        List<TupleWithJoinFlag> matchedRight = tempMap.get(leftKey);
                        if (matchedRight != null) {
                            for (TupleWithJoinFlag t : matchedRight) {
                                Object[] newTuple = Arrays.copyOf(leftTuple, leftLength + rightLength);
                                System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                                t.setJoined(true);
                                pushToNext(param, edge, param.getContext(), newTuple);
                            }
                        } else if (leftRequired) {
                            // LEFT or FULL join: emit left with null right
                            Object[] newTuple = Arrays.copyOf(leftTuple, leftLength + rightLength);
                            Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                            pushToNext(param, edge, param.getContext(), newTuple);
                        }
                    }
                }

                // Step 3: For RIGHT/FULL join: emit unmatched right tuples
                if (rightRequired) {
                    for (List<TupleWithJoinFlag> tList : tempMap.values()) {
                        for (TupleWithJoinFlag t : tList) {
                            if (!t.isJoined()) {
                                Object[] newTuple = new Object[leftLength + rightLength];
                                Arrays.fill(newTuple, 0, leftLength, null);
                                System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                                pushToNext(param, edge, param.getContext(), newTuple);
                            }
                        }
                    }
                }

                // Step 4: Clean up this partition's spill files
                if (rightSf != null) {
                    rightSf.close();
                }
                if (leftSf != null) {
                    leftSf.close();
                }
                tempMap.clear();

                LogUtils.debug(log, "Processed spilled partition {}", p);
            } catch (IOException e) {
                throw new RuntimeException("Failed to process spilled partition " + p, e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Existing helper methods
    // -------------------------------------------------------------------------

    private static void waitRightFinFlag(HashJoinParam param, Vertex vertex) {
        checkStatusError(param, vertex);
        try {
            param.getFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            param.interrupt();
            throw new RuntimeException("Wait for right side completion interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error while waiting for right side completion", e);
        }
        checkStatusError(param, vertex);
        if (!param.isRightFinFlag()) {
            throw new RuntimeException("Right fin flag not set after future completed");
        }
    }

    private static void checkStatusAndInterrupt(HashJoinParam param, Vertex vertex) {
        int status = vertex.getTask().getStatus();
        if (status == Status.STOPPED || status == Status.CANCEL) {
            LogUtils.warn(log, "Task status is {} ...", vertex.getTask().getStatus());
            param.interrupt();
        }
    }

    private static void checkStatusError(HashJoinParam param, Vertex vertex) {
        if (vertex.getTask().isLockWait()) {
            LogUtils.warn(log, "Task status is lock wait ...");
            throw new LockWaitException("Lock wait");
        } else {
            int status = vertex.getTask().getStatus();
            if (param.isInterrupted()) {
                throw new RuntimeException("HashJoin operation interrupted waiting");
            } else if (status == Status.STOPPED || status == Status.CANCEL) {
                LogUtils.warn(log, "Task status is {} ...", vertex.getTask().getStatus());
                param.interrupt();
                throw new RuntimeException("task is cancel");
            }
        }
    }

    private static boolean pushToNext(HashJoinParam param, Edge edge, Context context, Object[] tuple) {
        Object[] tmpTuple = param.rtrimTuple(tuple);
        if (param.getOtherExpr() != null) {
            Object object;
            synchronized (param.getOtherExpr()) {
                object = param.getOtherExpr().eval(tmpTuple);
            }
            if (object != null && (Boolean) object) {
                return edge.transformToNext(context, tuple);
            } else {
                return true;
            }
        } else {
            if (param.getRelOp() != null) {
                Object object;
                synchronized (param.getRelOp()) {
                    object = ((Object[]) ((PipeOp) param.getRelOp()).put(tmpTuple))[0];
                }
                if (object != null && (Boolean) object) {
                    return edge.transformToNext(context, tuple);
                } else {
                    return true;
                }
            }
            return edge.transformToNext(context, tuple);
        }
    }

    public static boolean isEmpty(TupleKey tupleKey, HashJoinParam param) {
        if (param.rightMappingEmpty && param.leftMappingEmpty) {
            return false;
        }
        Object[] tuple = tupleKey.getTuple();
        if (tuple != null) {
            for (Object item : tuple) {
                if (item != null) {
                    return false;
                }
            }
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // MemoryRevoker interface implementation
    // -------------------------------------------------------------------------

    @Override
    public ListenableFuture<?> startMemoryRevoke(AbstractParams param) {
        HashJoinParam hashJoinParam = (HashJoinParam) param;
        hashJoinParam.addSpillCnt(1);
        return spillToDisk(hashJoinParam);
    }

    @Override
    public void finishMemoryRevoke(AbstractParams param) {
        HashJoinParam hashJoinParam = (HashJoinParam) param;
        MemoryAllocatorCtx memoryAllocatorCtx = hashJoinParam.getMemoryAllocatorCtx();
        memoryAllocatorCtx.releaseRevocableMemory(memoryAllocatorCtx.getRevocableAllocated(), true);
        memoryAllocatorCtx.resetMemoryRevokingRequested();
        LogUtils.info(log, "HashJoinOperator finished memory revoke, released revocable memory");
    }

    @Override
    public MemoryAllocatorCtx getMemoryAllocatorCtx(AbstractParams param) {
        HashJoinParam hashJoinParam = (HashJoinParam) param;
        return hashJoinParam.getMemoryAllocatorCtx();
    }

    private ListenableFuture<?> spillToDisk(HashJoinParam hashJoinParam) {
        LogUtils.info(log, "HashJoinOperator start spill to disk, hashMap size: {}",
            hashJoinParam.getHashMap().size());
        SettableFuture<?> future = SettableFuture.create();
        SpillManager.getSpillExecutor().execute(() -> {
            try {
                hashJoinParam.spillPartitions();
                LogUtils.info(log, "HashJoinOperator spilled partitions during memory revocation");
                future.set(null);
            } catch (IOException e) {
                LogUtils.warn(log, "HashJoinOperator failed to spill: {}", e.getMessage());
                future.setException(e);
            }
        });
        return future;
    }
}
