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

package io.dingodb.calcite.grammar.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterRenameIndex extends SqlAlterTable {
    public SqlIdentifier tableId;
    public String originIndexName;

    public String toIndexName;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER TABLE RENAME INDEX", SqlKind.ALTER_TABLE);

    public SqlAlterRenameIndex(SqlParserPos pos, SqlIdentifier sqlIdentifier,
        String originIndexName, String toIndexName) {
        super(pos, sqlIdentifier, OPERATOR);
        this.tableId = sqlIdentifier;
        this.originIndexName = originIndexName;
        this.toIndexName = toIndexName;
    }

    @Override
    public void unparseAlterOperation(SqlWriter sqlWriter, int left, int right) {
        sqlWriter.keyword("ALTER TABLE ");
        tableId.unparse(sqlWriter, left, right);
        sqlWriter.keyword("RENAME");
        sqlWriter.keyword(originIndexName);
        sqlWriter.keyword("TO");
        sqlWriter.keyword(toIndexName);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

}
