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

import com.google.common.collect.Lists;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.vector.VectorCalcDistance;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.VectorPointDistanceParam;
import io.dingodb.tool.api.ToolService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
public class VectorPointDistanceOperator extends SoleOutOperator {

    public static final VectorPointDistanceOperator INSTANCE = new VectorPointDistanceOperator();

    public VectorPointDistanceOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        VectorPointDistanceParam param = vertex.getParam();
        param.setContext(context);
        param.getCache().add(tuple);
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        VectorPointDistanceParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("vectorPointDistance");
        long start = System.currentTimeMillis();
        TupleMapping selection = param.getSelection();
        List<Object[]> cache = param.getCache();
        if (fin instanceof FinWithException) {
            edge.fin(fin);
            return;
        }
        List<List<Float>> rightList = cache.stream().map(e ->
            (List<Float>) e[param.getVectorIndex()]
        ).collect(Collectors.toList());
        int topn = param.getTopk();
        if (rightList.isEmpty()) {
            edge.fin(fin);
            return;
        }
        List<Float> floatArray = new ArrayList<>();
        List<List<List<Float>>> partition = Lists.partition(rightList, 1024);
        for (List<List<Float>> right : partition) {
            VectorCalcDistance vectorCalcDistance = VectorCalcDistance.builder()
                .topN(topn)
                .leftList(Collections.singletonList(param.getTargetVector()))
                .rightList(right)
                .dimension(param.getDimension())
                .algorithmType(param.getAlgType())
                .metricType(param.getMetricType())
                .build();
            floatArray.addAll(ToolService.getDefault().vectorCalcDistance(
                param.getRangeDistribution().getId(),
                vectorCalcDistance).get(0));
        }
        TreeMap<Float, Object[]> map = new TreeMap<>(new Comparator<Float>() {
           @Override
           public int compare(Float f1, Float f2) {
                           return f2.compareTo(f1);
                       }
        });
        for (int i = 0; i < cache.size(); i ++) {
            Object[] tuple = cache.get(i);
            Object[] result = Arrays.copyOf(tuple, tuple.length + 1);
            result[tuple.length] = floatArray.get(i);
            map.put((Float) result[tuple.length], result);
        }
        int count = 0;
        Object[] value;
        for (Map.Entry<Float, Object[]> entry : map.entrySet()) {
            if (count < topn) {
                value = entry.getValue();
                edge.transformToNext(param.getContext(), selection.revMap(value));
            }
            count++;
        }

        param.clear();
        profile.time(start);
        edge.fin(fin);
    }
}
