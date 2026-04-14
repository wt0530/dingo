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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.ExecutionContext;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.profile.Profile;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.expr.DingoRelConfig;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.rel.RelOp;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Slf4j
@JsonTypeName("hashJoin")
@JsonPropertyOrder({"joinType", "leftMapping", "rightMapping"})
public class HashJoinParam extends AbstractParams implements RevokerParams {

    // -------------------------------------------------------------------------
    // Spill partition configuration
    // -------------------------------------------------------------------------
    public static final int NUM_PARTITIONS = 32;

    @JsonProperty("leftMapping")
    private final TupleMapping leftMapping;
    @JsonProperty("rightMapping")
    private final TupleMapping rightMapping;
    // For OUTER join, there may be no input tuples, so the length of tuple cannot be achieved.
    @JsonProperty("leftLength")
    private final int leftLength;
    @JsonProperty("rightLength")
    private final int rightLength;
    @JsonProperty("leftRequired")
    private final boolean leftRequired;
    @JsonProperty("rightRequired")
    private final boolean rightRequired;

    @Setter
    private transient boolean rightFinFlag;
    private transient ConcurrentHashMap<TupleKey, List<TupleWithJoinFlag>> hashMap;
    @Setter
    private transient CompletableFuture<Void> future;

    @Setter
    public Profile profileLeft;
    @Setter
    public Profile profileRight;

    @Setter
    public SqlExpr otherExpr;

    @Setter
    public RelOp relOp;
    public DingoRelConfig config;
    @Setter
    public DingoType schema;

    @Setter
    public String joinType;

    public boolean leftMappingEmpty;
    public boolean rightMappingEmpty;

    private volatile boolean interrupted = false;

    private ExecutionContext executionContext;

    protected long spillCnt = 0;
    OperatorMemoryAllocatorCtx memoryAllocatorCtx;
    AtomicLong size;
    /** Total number of tuples added to the build side (for spill threshold comparison). */
    private final AtomicLong tupleCount = new AtomicLong(0);
    private transient String jobId;
    private transient String operatorId;

    // -------------------------------------------------------------------------
    // Spill-to-disk state
    // -------------------------------------------------------------------------
    /** Left-side tuple schema for Avro serialization. */
    @Setter
    private DingoType leftSchema;
    /** Right-side tuple schema for Avro serialization. */
    @Setter
    private DingoType rightSchema;
    /** Which partitions have been spilled to disk. */
    private transient boolean[] spilledPartitions;
    /** Right-side spill files, one per spilled partition. */
    @Getter
    private transient TupleSpillFile[] rightSpillFiles;
    /** Left-side spill files, one per spilled partition. */
    @Getter
    private transient TupleSpillFile[] leftSpillFiles;


    public HashJoinParam(
        TupleMapping leftMapping,
        TupleMapping rightMapping,
        int leftLength,
        int rightLength,
        boolean leftRequired,
        boolean rightRequired,
        ExecutionContext executionContext
    ) {
        this.leftMapping = leftMapping;
        this.rightMapping = rightMapping;
        this.leftLength = leftLength;
        this.rightLength = rightLength;
        this.leftRequired = leftRequired;
        this.rightRequired = rightRequired;
        this.leftMappingEmpty = this.leftMapping.size() == 0;
        this.rightMappingEmpty = this.rightMapping.size() == 0;
        this.config = new DingoRelConfig();
        this.executionContext = executionContext;
    }

    public static TupleKey rtrimTupleKey(TupleKey key) {
        ArrayList<Object> arrayList = new ArrayList<>();
        Arrays.stream(key.getTuple()).forEach(
            obj -> {
                if (obj instanceof String) {
                    String str = (String) obj;
                    int blankCount = 0;
                    for ( int i = str.length() - 1; i >= 0; i-- ) {
                        if ( Character.isWhitespace(str.charAt(i)) ) {
                            blankCount++;
                        }
                    }
                    str = str.substring(0, str.length() - blankCount);
                    arrayList.add(str);
                } else {
                    arrayList.add(obj);
                }
            }
        );

        return new TupleKey(arrayList.toArray());
    }

    public static Object[] rtrimTuple(Object[] tuple) {
        ArrayList<Object> arrayList = new ArrayList<>();
        Arrays.stream(tuple).forEach(
            obj -> {
                if (obj instanceof String) {
                    String str = (String) obj;
                    int blankCount = 0;
                    for (int i = str.length() - 1; i >= 0; i--) {
                        if (Character.isWhitespace(str.charAt(i))) {
                            blankCount++;
                        } else {
                            break;
                        }
                    }
                    str = str.substring(0, str.length() - blankCount);
                    arrayList.add(str);
                } else {
                    arrayList.add(obj);
                }
            }
        );

        return arrayList.toArray();
    }

    public static boolean containsNull(TupleKey key) {
        for (Object item : key.getTuple()) {
            if (item == null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void init(Vertex vertex) {
        this.jobId =  vertex == null ? "hashJoinJobId" : vertex.getTask().getJobId().toString();
        this.operatorId = vertex == null ? "hashJoinOpId" : vertex.getOp().toString();
        rightFinFlag = false;
        hashMap = new ConcurrentHashMap<>();
        future = new CompletableFuture<>();
        if (relOp != null) {
            relOp = relOp.compile(new DingoCompileContext(
                (TupleType) schema.getType(),
                (TupleType) vertex.getParasType().getType()
            ), config);
        }
        this.size = new AtomicLong(0);
        if (!executionContext.isInnerSql()) {
            String name = "hashJoin" + UUID.randomUUID();
            MemoryPool memoryPool =
                MemoryPoolUtils.createOperatorTmpTablePool(name, executionContext.getMemoryPool());
            this.memoryAllocatorCtx = new OperatorMemoryAllocatorCtx(memoryPool, ScopeVariables.enableSpill());
        }
        // Initialize spill partition arrays
        spilledPartitions = new boolean[NUM_PARTITIONS];
        rightSpillFiles = new TupleSpillFile[NUM_PARTITIONS];
        leftSpillFiles = new TupleSpillFile[NUM_PARTITIONS];
    }

    public void clear() {
        rightFinFlag = false;
        hashMap.clear();
        future = new CompletableFuture<>();
        if (this.memoryAllocatorCtx != null) {
            this.memoryAllocatorCtx.close();
        }
        // Clean up spill files
        if (rightSpillFiles != null) {
            for (TupleSpillFile sf : rightSpillFiles) {
                if (sf != null) {
                    sf.close();
                }
            }
            rightSpillFiles = null;
        }
        if (leftSpillFiles != null) {
            for (TupleSpillFile sf : leftSpillFiles) {
                if (sf != null) {
                    sf.close();
                }
            }
            leftSpillFiles = null;
        }
        spilledPartitions = null;
    }

    public void interrupt() {
        this.interrupted = true;
        LogUtils.warn(log, "HashJoin operation interrupted");
        if (!future.isDone()) {
            future.completeExceptionally(new InterruptedException("HashJoin operation interrupted"));
        }
    }

    @Override
    public void addSpillCnt(int spillCnt) {
        this.spillCnt += spillCnt;
    }

    @Override
    public long getCacheSize() {
        return hashMap.size();
    }

    public void incMemSize(long size) {
        this.size.addAndGet(size);
    }

    /** Returns the total number of tuples added to the build side. */
    public long getTupleCount() {
        return tupleCount.get();
    }

    /** Increments the build-side tuple count by one. */
    public void incrementTupleCount() {
        tupleCount.incrementAndGet();
    }

    @Override
    public MemoryPool getQueryMemoryPool() {
        if (this.memoryAllocatorCtx != null) {
            return this.getExecutionContext().getMemoryPool();
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Spill-to-disk methods
    // -------------------------------------------------------------------------

    /** Returns whether spill-to-disk is enabled (both left and right schemas available). */
    public boolean isSpillEnabled() {
        return leftSchema != null && rightSchema != null;
    }

    /** Computes partition index for a given join key. */
    public int partitionOf(TupleKey key) {
        return Math.abs(key.hashCode()) % NUM_PARTITIONS;
    }

    /** Returns whether a specific partition has been spilled. */
    public boolean isPartitionSpilled(int partition) {
        return spilledPartitions != null && spilledPartitions[partition];
    }

    /** Returns whether any partition has been spilled to disk. */
    public boolean hasSpilledPartitions() {
        if (spilledPartitions == null) {
            return false;
        }
        for (boolean spilled : spilledPartitions) {
            if (spilled) {
                return true;
            }
        }
        return false;
    }

    /**
     * Spills all current hashMap entries to disk, partitioned by key hash.
     * Entries are removed from hashMap after writing. Right spill files are kept
     * open for potential append from subsequent right tuples.
     *
     * @throws IOException if spill files cannot be created or written
     */
    public synchronized void spillPartitions() throws IOException {
        if (!isSpillEnabled() || hashMap.isEmpty()) {
            return;
        }
        // Write each hashMap entry to its corresponding partition's right spill file
        for (Map.Entry<TupleKey, List<TupleWithJoinFlag>> entry : hashMap.entrySet()) {
            TupleKey key = entry.getKey();
            int p = partitionOf(key);
            if (rightSpillFiles[p] == null) {
                rightSpillFiles[p] = new TupleSpillFile(
                    SpillManager.INSTANCE.createSpillFile(jobId, operatorId), rightSchema);
            }
            List<Object[]> tuples = new ArrayList<>(entry.getValue().size());
            for (TupleWithJoinFlag twjf : entry.getValue()) {
                tuples.add(twjf.getTuple());
            }
            rightSpillFiles[p].write(tuples);
            spilledPartitions[p] = true;
        }
        long spilledSize = hashMap.size();
        hashMap.clear();
        tupleCount.set(0);
        LogUtils.debug(log, "Spilled {} hash groups across partitions", spilledSize);
    }

    /**
     * Writes a right-side tuple directly to a spilled partition's file.
     * Called when a new right tuple arrives for an already-spilled partition.
     */
    public synchronized void spillRightTuple(int partition, Object[] tuple) throws IOException {
        if (rightSpillFiles[partition] == null) {
            rightSpillFiles[partition] = new TupleSpillFile(
                SpillManager.INSTANCE.createSpillFile(jobId, operatorId), rightSchema);
            spilledPartitions[partition] = true;
        }
        rightSpillFiles[partition].write(Collections.singletonList(tuple));
    }

    /**
     * Writes a left-side tuple to a spilled partition's file for later processing.
     */
    public synchronized void spillLeftTuple(int partition, Object[] tuple) throws IOException {
        if (leftSpillFiles[partition] == null) {
            leftSpillFiles[partition] = new TupleSpillFile(
                SpillManager.INSTANCE.createSpillFile(jobId, operatorId), leftSchema);
        }
        leftSpillFiles[partition].write(Collections.singletonList(tuple));
    }

    /**
     * Finalizes all open right spill files. Called in fin(pin=1) after all right
     * tuples have been received, before signaling right completion.
     */
    public synchronized void finishRightSpillFiles() throws IOException {
        if (rightSpillFiles == null) {
            return;
        }
        for (TupleSpillFile sf : rightSpillFiles) {
            if (sf != null) {
                sf.finishWrite();
            }
        }
    }
}
