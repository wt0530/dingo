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

package io.dingodb.calcite.grammar.dml;

import lombok.Getter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * add trace
 */
public class SqlInsert extends org.apache.calcite.sql.SqlInsert {

    @Getter
    private boolean trace;
    @Getter
    private @Nullable SqlNodeList sourceExpressionList;
    @Getter
    private @Nullable SqlNodeList targetColumnList2;

    public SqlInsert(SqlParserPos pos,
                     SqlNodeList keywords,
                     SqlNode targetTable,
                     SqlNode source,
                     @Nullable SqlNodeList columnList,
                     @Nullable boolean trace) {
        super(pos, keywords, targetTable, source, columnList);
        this.trace = trace;
    }

    public SqlInsert(SqlParserPos pos,
                     SqlNodeList keywords,
                     SqlNode targetTable,
                     SqlNode source,
                     @Nullable SqlNodeList columnList,
                     @Nullable SqlNodeList sourceExpressionList,
                     @Nullable SqlNodeList targetColumnList) {
        super(pos, keywords, targetTable, source, columnList);
        this.sourceExpressionList = sourceExpressionList;
        this.targetColumnList2 = targetColumnList;
    }

}
