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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryManager;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.memory.QueryMemoryPool;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.aggregate.AbstractAgg;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.aggregate.AggCache;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

@Slf4j
@JsonTypeName("aggregate")
@JsonPropertyOrder({"keys", "aggregates", "schema", "spillThreshold"})
public class AggregateParams extends AbstractParams implements RevokerParams {

    @JsonProperty("keys")
    private final TupleMapping keyMapping;

    @JsonProperty("aggregates")
    @JsonSerialize(contentAs = AbstractAgg.class)
    @JsonDeserialize(contentAs = AbstractAgg.class)
    private final List<Agg> aggList;

    /**
     * Schema for serializing spilled AggCache entries (format: [key_cols, agg_state_values]).
     * This should be the aggregate node's output row type.
     * When {@code null}, spill-to-disk is disabled.
     */
    @JsonProperty("schema")
    private final DingoType schema;

    /**
     * Maximum number of distinct group keys in AggCache before spilling to disk.
     * A value of {@code 0} means "use the default from {@link SpillManager}".
     */
    @JsonProperty("spillThreshold")
    private final int spillThreshold;

    @Getter
    private transient AggCache cache;
    /** Spill files containing partially-aggregated entries; {@code null} when spill is disabled. */
    private transient List<TupleSpillFile> spillFiles;
    /** Total number of entries already written to spill files. */
    private transient long spilledCount;
    /** Per-operator query-level memory pool (for scheduler integration). */
    private transient QueryMemoryPool queryMemoryPool;
    /** Memory allocator context used by the memory-revoking scheduler. */
    private transient OperatorMemoryAllocatorCtx memoryAllocatorCtx;

    private transient String jobId;
    private transient String operatorId;

    protected long spillCnt = 0;

    @JsonCreator
    public AggregateParams(
        @JsonProperty("keys") TupleMapping keyMapping,
        @JsonProperty("aggregates") List<Agg> aggList,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("spillThreshold") int spillThreshold
    ) {
        this.keyMapping = keyMapping;
        this.aggList = aggList;
        this.schema = schema;
        this.spillThreshold = spillThreshold;
    }

    /** Convenience constructor for callers that do not need spill support (backward compat). */
    public AggregateParams(TupleMapping keyMapping, List<Agg> aggList) {
        this(keyMapping, aggList, null, 0);
    }

    @Override
    public void init(Vertex vertex) {
        this.jobId = vertex.getTask().getJobId().toString();
        this.operatorId = vertex.getOp().toString();
        cache = new AggCache(keyMapping, aggList);
        if (schema != null) {
            spillFiles = new ArrayList<>();
            spilledCount = 0;
            if (ScopeVariables.enableSpill()) {
                String poolName = "aggregate-" + UUID.randomUUID();
                queryMemoryPool = (QueryMemoryPool) MemoryManager.getInstance()
                    .createQueryMemoryPool(false, poolName);
                MemoryPool opPool = MemoryPoolUtils.createOperatorTmpTablePool(
                    poolName + "-op", queryMemoryPool);
                memoryAllocatorCtx = new OperatorMemoryAllocatorCtx(opPool, true);
            }
        }
    }

    /**
     * Adds an input tuple. Always aggregates directly into AggCache for maximum
     * memory efficiency. When spill is enabled, checks if AggCache has grown beyond
     * the threshold and spills partial aggregates to disk if needed.
     */
    public synchronized void addTuple(Object[] tuple) {
        cache.addTuple(tuple);
        if (schema != null) {
            boolean shouldSpill = cache.size() >= getEffectiveSpillThreshold()
                || (memoryAllocatorCtx != null && memoryAllocatorCtx.isMemoryRevokingRequested());
            if (shouldSpill) {
                try {
                    spillAggCache();
                    if (memoryAllocatorCtx != null) {
                        memoryAllocatorCtx.releaseRevocableMemory(
                            memoryAllocatorCtx.getRevocableAllocated(), true);
                        memoryAllocatorCtx.resetMemoryRevokingRequested();
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to spill AggCache to disk", e);
                }
            }
        }
    }

    /**
     * Merges all spilled partial aggregates back into the current AggCache using
     * {@link AggCache#reduce(Object[])}, making final results available via
     * {@link #getCache()}. This is a no-op when spill is disabled or no spill files exist.
     *
     * <p>Each spill file is streamed one-tuple-at-a-time and closed immediately after
     * reading to release file handles and disk space.
     *
     * @throws IOException if a spill file cannot be read
     */
    public void prepareResults() throws IOException {
        if (schema == null || spillFiles == null || spillFiles.isEmpty()) {
            return;
        }
        // Merge all spill files (partially-aggregated entries) into the current AggCache
        // using reduce() which calls Agg.merge() to combine partial aggregate states.
        for (TupleSpillFile sf : spillFiles) {
            try {
                Iterator<Object[]> it = sf.iterator();
                while (it.hasNext()) {
                    cache.reduce(it.next());
                }
            } finally {
                sf.close();
            }
        }
        spillFiles.clear();
        spilledCount = 0;
    }

    /** Returns the effective spill threshold (always positive). */
    public int getEffectiveSpillThreshold() {
        return spillThreshold > 0 ? spillThreshold : SpillManager.DEFAULT_SPILL_THRESHOLD;
    }

    /** Returns whether spill-to-disk is enabled for this parameter set. */
    public boolean isSpillEnabled() {
        return schema != null;
    }

    public void clear() {
        cache.clear();
        spilledCount = 0;
        if (spillFiles != null) {
            for (TupleSpillFile sf : spillFiles) {
                sf.close();
            }
            spillFiles.clear();
        }
        if (memoryAllocatorCtx != null) {
            memoryAllocatorCtx.releaseRevocableMemory(memoryAllocatorCtx.getRevocableAllocated(), true);
        }
        if (queryMemoryPool != null) {
            queryMemoryPool.destroy();
            queryMemoryPool = null;
        }
    }

    @Override
    public MemoryPool getQueryMemoryPool() {
        return queryMemoryPool;
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocatorCtx;
    }

    // -------------------------------------------------------------------------

    /**
     * Drains the current AggCache entries (partially-aggregated results) to a spill file
     * and clears the cache. The spilled format is {@code [key_cols, agg_state_values]}
     * which can later be merged back via {@link AggCache#reduce(Object[])}.
     */
    public synchronized void spillAggCache() throws IOException {
        if (cache.size() == 0) {
            return;
        }
        List<Object[]> entries = cache.drainEntries();
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(jobId, operatorId), schema);
        sf.write(entries);
        sf.finishWrite();
        spilledCount += entries.size();
        LogUtils.debug(log, "Spilled {} aggregate cache entries to {}, totalSpilled={}",
            entries.size(), sf.getFile().getName(), spilledCount);
        spillFiles.add(sf);
    }

    @Override
    public void addSpillCnt(int spillCnt) {
        this.spillCnt += spillCnt;
    }

    @Override
    public long getCacheSize() {
        return cache.size();
    }
}
