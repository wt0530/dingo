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
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import io.dingodb.common.vector.VectorCalcDistance;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.TaskStatus;
import io.dingodb.exec.memory.MemoryRevoker;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.AbstractParams;
import io.dingodb.exec.operator.params.VectorPointDistanceParam;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.tool.api.MemoryAllocatorCtx;
import io.dingodb.tool.api.ToolService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static io.dingodb.exec.transaction.util.BinaryVectorUtils.getBinaryVectorList;

@Slf4j
public class VectorPointDistanceOperator extends SoleOutOperator implements MemoryRevoker {

    public static final VectorPointDistanceOperator INSTANCE = new VectorPointDistanceOperator();

    public VectorPointDistanceOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        VectorPointDistanceParam param = vertex.getParam();
        // Synchronize on param to prevent concurrent modification with the
        // memory-revoke thread which also calls spillCurrentBatch()
        synchronized (param) {
            param.setContext(context);
            param.getCache().add(tuple);
            // Spill to disk when the in-memory buffer reaches the configured threshold,
            // or when the memory revoking scheduler requests it
            boolean shouldSpill = param.isSpillEnabled()
                && (param.getCache().size() >= param.getEffectiveSpillThreshold()
                || (param.getMemoryAllocatorCtx() != null
                && param.getMemoryAllocatorCtx().isMemoryRevokingRequested()));
            if (shouldSpill) {
                try {
                    param.spillCurrentBatch();
                    if (param.getMemoryAllocatorCtx() != null) {
                        param.getMemoryAllocatorCtx().releaseRevocableMemory(
                            param.getMemoryAllocatorCtx().getRevocableAllocated(), true);
                        param.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to spill VectorPointDistance buffer to disk", e);
                }
            }
        }
        return true;
    }

    private static final int DISTANCE_BATCH_SIZE = 1024;

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        VectorPointDistanceParam param = vertex.getParam();
        synchronized (param) {
            Edge edge = vertex.getSoleEdge();
            if (fin instanceof FinWithException) {
                edge.fin(fin);
                return;
            }
            OperatorProfile profile = param.getProfile("vectorPointDistance");
            long start = System.currentTimeMillis();
            TupleMapping selection = param.getSelection();
            int topn = param.getTopk();

            // Stream tuples from spill files + cache in batches,
            // calculate distances per batch, maintain only topN results in memory.
            Iterator<Object[]> allTuples;
            try {
                allTuples = param.tupleIterator();
            } catch (IOException e) {
                LogUtils.error(log, "Failed to open spill files for VectorPointDistance: {}", e.getMessage(), e);
                TaskStatus taskStatus = new TaskStatus();
                taskStatus.setStatus(false);
                taskStatus.setTaskId(vertex.getTask().getId().toString());
                taskStatus.setErrorMsg(e.getMessage());
                edge.fin(FinWithException.of(taskStatus));
                return;
            }

            if (!allTuples.hasNext()) {
                param.clear();
                profile.time(start);
                edge.fin(fin);
                return;
            }

            // Max-heap by distance: the largest distance sits at the top for easy eviction
            PriorityQueue<Pair<Float, Object[]>> topNHeap = new PriorityQueue<>(
                topn + 1,
                (a, b) -> Float.compare(b.getKey(), a.getKey())
            );

            if (!param.isBinaryVector()) {
                processFloatVectorBatches(allTuples, param, topn, topNHeap);
            } else {
                processBinaryVectorBatches(allTuples, param, topn, topNHeap);
            }

            // Sort topN results by distance ascending and emit
            List<Pair<Float, Object[]>> sorted = new ArrayList<>(topNHeap);
            sorted.sort(Comparator.comparing(Pair::getKey));
            for (Pair<Float, Object[]> pair : sorted) {
                edge.transformToNext(param.getContext(), selection.revMap(pair.getValue()));
            }

            param.clear();
            profile.time(start);
            edge.fin(fin);
        }
    }

    /**
     * Processes float vectors in batches: reads tuples from iterator in chunks of
     * {@link #DISTANCE_BATCH_SIZE}, calculates distances, and maintains the topN heap.
     * Memory usage is bounded to O(DISTANCE_BATCH_SIZE + topN).
     */
    private static void processFloatVectorBatches(
        Iterator<Object[]> allTuples,
        VectorPointDistanceParam param,
        int topn,
        PriorityQueue<Pair<Float, Object[]>> topNHeap
    ) {
        List<Object[]> batch = new ArrayList<>(DISTANCE_BATCH_SIZE);
        while (allTuples.hasNext()) {
            batch.add(allTuples.next());
            if (batch.size() >= DISTANCE_BATCH_SIZE) {
                calcFloatDistancesAndMerge(batch, param, topn, topNHeap);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            calcFloatDistancesAndMerge(batch, param, topn, topNHeap);
        }
    }

    private static void calcFloatDistancesAndMerge(
        List<Object[]> batch,
        VectorPointDistanceParam param,
        int topn,
        PriorityQueue<Pair<Float, Object[]>> topNHeap
    ) {
        List<List<Float>> rightList = batch.stream()
            .map(e -> (List<Float>) e[param.getVectorIndex()])
            .collect(Collectors.toList());

        VectorCalcDistance vectorCalcDistance = VectorCalcDistance.builder()
            .topN(topn)
            .isBinaryVector(false)
            .leftList(Collections.singletonList(param.getTargetVector()))
            .rightList(rightList)
            .dimension(param.getDimension())
            .algorithmType(param.getAlgType())
            .metricType(param.getMetricType())
            .build();

        List<Float> distances = ToolService.getDefault().vectorCalcDistance(
            param.getRangeDistribution().getId(), vectorCalcDistance).get(0);

        for (int i = 0; i < batch.size(); i++) {
            Object[] tuple = batch.get(i);
            Object[] result = Arrays.copyOf(tuple, tuple.length + 1);
            float dist = distances.get(i);
            result[tuple.length] = dist;
            topNHeap.add(new Pair<>(dist, result));
            if (topNHeap.size() > topn) {
                topNHeap.poll(); // evict the largest distance
            }
        }
    }

    /**
     * Processes binary vectors one by one: calculates distance for each tuple
     * and maintains the topN heap. Memory usage is bounded to O(topN).
     */
    private static void processBinaryVectorBatches(
        Iterator<Object[]> allTuples,
        VectorPointDistanceParam param,
        int topn,
        PriorityQueue<Pair<Float, Object[]>> topNHeap
    ) {
        List<byte[]> leftBinaryValues = getBinaryVectorList(param.getBinaryVector(), param.getDimension());
        while (allTuples.hasNext()) {
            Object[] tuple = allTuples.next();
            byte[] rightVector = (byte[]) tuple[param.getVectorIndex()];
            List<byte[]> rightBinaryValues = getBinaryVectorList(rightVector, param.getDimension());

            VectorCalcDistance vectorCalcDistance = VectorCalcDistance.builder()
                .topN(topn)
                .isBinaryVector(true)
                .leftBinaryValues(leftBinaryValues)
                .rightBinaryValues(rightBinaryValues)
                .dimension(param.getDimension())
                .algorithmType(param.getAlgType())
                .metricType(param.getMetricType())
                .build();

            List<Float> distances = ToolService.getDefault().vectorCalcDistance(
                param.getRangeDistribution().getId(), vectorCalcDistance).get(0);

            Object[] result = Arrays.copyOf(tuple, tuple.length + 1);
            float dist = distances.get(0);
            result[tuple.length] = dist;
            topNHeap.add(new Pair<>(dist, result));
            if (topNHeap.size() > topn) {
                topNHeap.poll();
            }
        }
    }

    // -------------------------------------------------------------------------
    // MemoryRevoker interface implementation
    // -------------------------------------------------------------------------

    @Override
    public ListenableFuture<?> startMemoryRevoke(AbstractParams param) {
        VectorPointDistanceParam vpParam = (VectorPointDistanceParam) param;
        SettableFuture<?> future = SettableFuture.create();
        SpillManager.getSpillExecutor().execute(() -> {
            try {
                vpParam.spillCurrentBatch();
                LogUtils.info(log, "VectorPointDistanceOperator spilled current batch during memory revocation");
                future.set(null);
            } catch (IOException e) {
                LogUtils.warn(log, "VectorPointDistanceOperator failed to spill during memory revocation: {}",
                    e.getMessage());
                future.setException(e);
            }
        });
        return future;
    }

    @Override
    public void finishMemoryRevoke(AbstractParams param) {
        VectorPointDistanceParam vpParam = (VectorPointDistanceParam) param;
        if (vpParam.getMemoryAllocatorCtx() != null) {
            vpParam.getMemoryAllocatorCtx().releaseRevocableMemory(
                vpParam.getMemoryAllocatorCtx().getRevocableAllocated(), true);
            vpParam.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
            LogUtils.info(log, "VectorPointDistanceOperator finished memory revoke, released revocable memory");
        }
    }

    @Override
    public MemoryAllocatorCtx getMemoryAllocatorCtx(AbstractParams param) {
        VectorPointDistanceParam vpParam = (VectorPointDistanceParam) param;
        return vpParam.getMemoryAllocatorCtx();
    }
}
