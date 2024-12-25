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

package io.dingodb.calcite.rule;

import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.logical.LogicalTableModify;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class DingoSpecialInsertRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalTableModify.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoSpecialInsertRule"
        )
        .withRuleFactory(DingoSpecialInsertRule::new);

    protected DingoSpecialInsertRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalTableModify modify = (LogicalTableModify) rel;
        if (Objects.requireNonNull(modify.getOperation()) != TableModify.Operation.INSERT) {
            throw new IllegalStateException(
                "Operation \"" + modify.getOperation() + "\" is not supported."
            );
        }
        RelTraitSet traits = modify.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(null, modify.getTable()));
        return new DingoTableModify(
            modify.getCluster(),
            traits,
            modify.getTable(),
            modify.getCatalogReader(),
            convert(modify.getInput(), traits),
            modify.getOperation(),
            modify.getUpdateColumnList(),
            modify.getSourceExpressionList(),
            modify.isFlattened(),
            modify.getTargetColumnNames(),
            modify.getSourceExpressionList2()
        );
    }
}
