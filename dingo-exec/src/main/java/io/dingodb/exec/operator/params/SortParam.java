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
import io.dingodb.common.ExecutionContext;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.memory.QueryMemoryPool;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Getter
@JsonTypeName("sort")
@JsonPropertyOrder({"collations", "limit", "offset", "vectorHybrid", "schema", "spillThreshold"})
public class SortParam extends AbstractParams implements RevokerParams {

    @JsonProperty("collations")
    private final List<SortCollation> collations;
    @JsonProperty("limit")
    private final int limit;
    @JsonProperty("offset")
    private final int offset;
    @JsonProperty("vectorHybrid")
    private final boolean vectorHybrid;

    /**
     * 用于序列化溢出数据的可选 tuple schema。
     * 当 {@code null} 时，溢出到磁盘被禁用。
     */
    @JsonProperty("schema")
    private final DingoType schema;
    /**
     * 在排序运行溢出到磁盘之前内存中保存的最大 tuple 数。
     * 值 {@code 0} 表示“使用 {@link SpillManager} 中的默认值”。
     */
    @JsonProperty("spillThreshold")
    private final int spillThreshold;

    /** 内存中 tuple 缓冲区. */
    private final List<Object[]> cache;

    /** 写入磁盘的排序 runs 的溢出文件； {@code null} 当溢出被禁用时。 */
    private transient List<TupleSpillFile> spillFiles;
    /** 已写入溢出文件的 tuple 总数. */
    private transient long spilledCount;
    /** 每个operator查询级内存池（用于调度程序集成）。 */
    private transient QueryMemoryPool queryMemoryPool;
    /** 内存撤销调度程序使用的内存分配器上下文。 */
    private transient OperatorMemoryAllocatorCtx memoryAllocatorCtx;

    private transient Comparator<Object[]> comparator;

    private ExecutionContext executionContext;
    private AtomicLong size;
    private transient String jobId;
    private transient String operatorId;

    protected long spillCnt = 0;

    @JsonCreator
    public SortParam(
        @JsonProperty("collations") @NonNull List<SortCollation> collations,
        @JsonProperty("limit") int limit,
        @JsonProperty("offset") int offset,
        @JsonProperty("vectorHybrid") boolean vectorHybrid,
        @JsonProperty("executionContext") ExecutionContext executionContext,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("spillThreshold") int spillThreshold
    ) {
        this.collations = collations;
        this.limit = limit;
        this.offset = offset;
        this.vectorHybrid = vectorHybrid;
        this.schema = schema;
        this.spillThreshold = spillThreshold;
        this.cache = new ArrayList<>();
        this.spilledCount = 0;
        if (schema != null) {
            this.spillFiles = new ArrayList<>();
        }
        comparator = buildComparator(collations);
        this.executionContext = executionContext;
        this.size = new AtomicLong(0);
    }

    public SortParam(
        @JsonProperty("collations") @NonNull List<SortCollation> collations,
        @JsonProperty("limit") int limit,
        @JsonProperty("offset") int offset,
        @JsonProperty("vectorHybrid") boolean vectorHybrid,
        @JsonProperty("executionContext") ExecutionContext executionContext
    ) {
        this(collations, limit, offset, vectorHybrid, executionContext, null, 0);
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        this.jobId = vertex.getTask().getJobId().toString();
        this.operatorId = vertex.getOp().toString();
        comparator = buildComparator(collations);
        if (schema != null) {
            spillFiles = new ArrayList<>();
            spilledCount = 0;
        }
        if (!executionContext.isInnerSql()) {
            String poolName = "sort-" + UUID.randomUUID();
            queryMemoryPool = (QueryMemoryPool) executionContext.getMemoryPool();
            MemoryPool opPool = MemoryPoolUtils.createOperatorTmpTablePool(
                poolName + "-op", queryMemoryPool);
            memoryAllocatorCtx = new OperatorMemoryAllocatorCtx(opPool, ScopeVariables.enableSpill());
        }
    }

    /**
     * 返回有效溢出阈值（始终为正）。
     */
    public int getEffectiveSpillThreshold() {
        return spillThreshold > 0 ? spillThreshold : SpillManager.DEFAULT_SPILL_THRESHOLD;
    }

    /**
     * 返回是否为此参数集启用溢出到磁盘。
     */
    public boolean isSpillEnabled() {
        return schema != null;
    }

    /**
     * 返回是否有任何 tuple 已溢出到磁盘。
     */
    public boolean hasSpillFiles() {
        return spillFiles != null && !spillFiles.isEmpty();
    }

    /**
     * 对当前内存缓存进行排序，并将其作为排序 run 写入新的溢出文件。
     * 之后清除内存缓存。
     *
     * @throws IOException 如果无法创建或写入溢出文件
     */
    public synchronized void spillCurrentBatch() throws IOException {
        if (cache.isEmpty()) {
            return;
        }
        if (comparator != null) {
            cache.sort(comparator);
        }
        TupleSpillFile spillFile = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(jobId, operatorId), schema);
        spillFile.write(cache);
        spillFile.finishWrite();
        spilledCount += cache.size();
        LogUtils.debug(log, "Spilled {} tuples to {}, totalSpilled={}", cache.size(),
            spillFile.getFile().getName(), spilledCount);
        spillFiles.add(spillFile);
        cache.clear();
    }

    /**
     * 返回已溢出到磁盘的 tuple 总数（不包括
     * 当前内存中的批次）。
     */
    public long getSpilledCount() {
        return spilledCount;
    }

    /**
     * 清除内存缓存并关闭/删除所有溢出文件。
     */
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
        // queryMemoryPool is borrowed from executionContext, not owned by this operator — do not destroy it
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocatorCtx;
    }

    public OperatorProfile getProfile() {
        return new OperatorProfile("sort");
    }

    // -------------------------------------------------------------------------

    private static Comparator<Object[]> buildComparator(List<SortCollation> collations) {
        if (collations.isEmpty()) {
            return null;
        }
        Comparator<Object[]> c = collations.get(0).makeComparator();
        for (int i = 1; i < collations.size(); ++i) {
            c = c.thenComparing(collations.get(i).makeComparator());
        }
        return c;
    }

    @Override
    public MemoryPool getQueryMemoryPool() {
        if (this.memoryAllocatorCtx != null && this.getExecutionContext() != null) {
            return this.getExecutionContext().getMemoryPool();
        }
        return null;
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
