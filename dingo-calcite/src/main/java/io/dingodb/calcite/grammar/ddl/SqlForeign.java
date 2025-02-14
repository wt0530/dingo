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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlForeign extends SqlCall {

    SqlIdentifier table;
    SqlIdentifier foreignName;
    SqlNodeList columnList;
    SqlNodeList refColumnList;
    SqlIdentifier refTable;

    String updateRefOpt;
    String deleteRefOpt;

    private static final SqlSpecialOperator OPERATOR =
        new SqlSpecialOperator("CONSTRAINT FOREIGN", SqlKind.FOREIGN_KEY);

    public SqlForeign(
        SqlParserPos pos,
        SqlIdentifier foreignName,
        SqlNodeList columnList,
        SqlIdentifier refTable,
        SqlNodeList refColumnList,
        String updateRefOpt,
        String deleteRefOpt
    ) {
        super(pos);
        this.foreignName = foreignName;
        this.columnList = columnList;
        this.refTable = refTable;
        this.refColumnList = refColumnList;
        this.updateRefOpt = updateRefOpt;
        this.deleteRefOpt = deleteRefOpt;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CONSTRAINT FOREIGN KEY");
        if (foreignName != null) {
            foreignName.unparse(writer, leftPrec, rightPrec);
        }
        columnList.unparse(writer, leftPrec, rightPrec);
        writer.keyword("reference");
        refTable.unparse(writer, leftPrec, rightPrec);
        refColumnList.unparse(writer, leftPrec, rightPrec);
        if (updateRefOpt != null) {
            writer.keyword("on update");
            writer.keyword(updateRefOpt);
        }
        if (deleteRefOpt != null) {
            writer.keyword("on delete");
            writer.keyword(deleteRefOpt);
        }
    }
}
