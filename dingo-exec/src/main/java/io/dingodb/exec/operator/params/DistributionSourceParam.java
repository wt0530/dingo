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
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@JsonTypeName("distributionSource")
@JsonPropertyOrder({
    "startKey", "endKey", "withStart", "withEnd", "concurrencyLevel"
})
public class DistributionSourceParam extends SourceParam {

    private final Table td;
    @Setter
    private NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution;
    @JsonProperty("startKey")
    private final byte[] startKey;
    @JsonProperty("endKey")
    private final byte[] endKey;
    @JsonProperty("withStart")
    private final boolean withStart;
    @JsonProperty("withEnd")
    private final boolean withEnd;
    @Setter
    private PartitionService ps;
    private SqlExpr filter;
    private boolean logicalNot;
    private boolean notBetween;
    private Object[] keyTuple;
    @Setter
    private boolean filterRange;
    @Setter
    private int keepOrder;
    @JsonProperty("concurrencyLevel")
    private final int concurrencyLevel;
    @Setter
    private Map<CommonId, Integer> splitRetry = new ConcurrentHashMap<>();

    public DistributionSourceParam(
        Table td,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution,
        byte[] startKey,
        byte[] endKey,
        boolean withStart,
        boolean withEnd,
        SqlExpr filter,
        boolean logicalNot,
        boolean notBetween,
        Object[] keyTuple,
        int concurrencyLevel
    ) {
        this.td = td;
        this.rangeDistribution = rangeDistribution;
        this.startKey = startKey;
        this.endKey = endKey;
        this.withStart = withStart;
        this.withEnd = withEnd;
        this.filter = filter;
        this.logicalNot = logicalNot;
        this.notBetween = notBetween;
        this.keyTuple = keyTuple;
        this.concurrencyLevel = concurrencyLevel;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        ps = PartitionService.getService(
            Optional.ofNullable(td.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
    }

    public DistributionSourceParam copy(
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution,
        byte[] start,
        byte[] end,
        boolean withStart,
        boolean withEnd) {
        DistributionSourceParam param = new DistributionSourceParam(
            this.td,
            rangeDistribution,
            start,
            end,
            withStart,
            withEnd,
            this.filter,
            this.logicalNot,
            this.notBetween,
            this.keyTuple,
            this.concurrencyLevel);
        param.setPs(this.ps);
        return param;
    }

}
