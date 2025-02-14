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

package org.apache.calcite.sql.ddl;

import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoSqlColumn extends SqlColumnDeclaration {
    @Getter boolean autoIncrement;

    @Getter
    @Setter
    boolean primaryKey;

    @Getter
    @Setter
    String comment = "";

    @Getter
    String collate = "utf8_bin";

    public DingoSqlColumn(
        SqlParserPos pos,
        SqlIdentifier name,
        SqlDataTypeSpec dataType,
        @Nullable SqlNode expression,
        ColumnStrategy strategy,
        boolean autoIncrement,
        String comment,
        boolean primary,
        String collate
    ) {
        super(pos, name, dataType, expression, strategy);
        this.autoIncrement = autoIncrement;
        this.comment = comment;
        this.primaryKey = primary;
        this.collate = collate;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, 0, 0);
        dataType.unparse(writer, 0, 0);
        if (Boolean.FALSE.equals(dataType.getNullable())) {
            writer.keyword("NOT NULL");
        }
    }
}
