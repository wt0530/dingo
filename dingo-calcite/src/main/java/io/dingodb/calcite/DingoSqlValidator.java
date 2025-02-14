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

package io.dingodb.calcite;

import io.dingodb.calcite.fun.DingoOperatorTable;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.TableDiskAnnFunctionNamespace;
import org.apache.calcite.sql.validate.TableFunctionNamespace;
import org.apache.calcite.sql.validate.TableHybridFunctionNamespace;
import org.apache.calcite.sql2rel.SqlDiskAnnOperator;
import org.apache.calcite.sql2rel.SqlDocumentOperator;
import org.apache.calcite.sql2rel.SqlFunctionScanOperator;
import org.apache.calcite.sql2rel.SqlHybridSearchOperator;
import org.apache.calcite.sql2rel.SqlVectorOperator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DingoSqlValidator extends SqlValidatorImpl {

    @Getter
    @Setter
    private boolean hybridSearch;
    @Getter
    @Setter
    private String hybridSearchSql;
    @Getter
    private Map<SqlBasicCall, String> hybridSearchMap;

    static Config CONFIG = Config.DEFAULT
        .withConformance(DingoParser.PARSER_CONFIG.conformance());

    DingoSqlValidator(
        DingoCatalogReader catalogReader,
        RelDataTypeFactory typeFactory
    ) {
        super(
            SqlOperatorTables.chain(
                SqlStdOperatorTable.instance(),
                DingoOperatorTable.instance(),
                catalogReader
            ),
            catalogReader,
            typeFactory,
            DingoSqlValidator.CONFIG
        );
        this.hybridSearch = false;
        this.hybridSearchSql = "";
        this.hybridSearchMap = new ConcurrentHashMap<>();
    }

    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        super.validateCall(call, scope);
    }

    @Override
    protected void registerNamespace(
        @Nullable SqlValidatorScope usingScope, @Nullable String alias, SqlValidatorNamespace ns, boolean forceNullable
    ) {
        SqlNode enclosingNode = ns.getEnclosingNode();
        if (enclosingNode instanceof SqlBasicCall
            && (((SqlBasicCall) enclosingNode).getOperator() instanceof SqlFunctionScanOperator
            || ((SqlBasicCall) enclosingNode).getOperator() instanceof SqlVectorOperator
            || ((SqlBasicCall) enclosingNode).getOperator() instanceof SqlDocumentOperator)
        ) {
            super.registerNamespace(
                usingScope, alias,
                new TableFunctionNamespace(this, (SqlBasicCall) enclosingNode),
                forceNullable
            );
            return;
        } else if (enclosingNode instanceof SqlBasicCall
            && (((SqlBasicCall) enclosingNode).getOperator() instanceof SqlHybridSearchOperator)
        ) {
            super.registerNamespace(
                usingScope, alias,
                new TableHybridFunctionNamespace(this, (SqlBasicCall) enclosingNode),
                forceNullable
            );
            return;
        } else if (enclosingNode instanceof SqlBasicCall
            && (((SqlBasicCall) enclosingNode).getOperator() instanceof SqlDiskAnnOperator)
        ) {
            super.registerNamespace(
                usingScope, alias,
                new TableDiskAnnFunctionNamespace(this, (SqlBasicCall) enclosingNode),
                forceNullable
            );
            return;
        }
        super.registerNamespace(usingScope, alias, ns, forceNullable);
    }

    @Override
    public @Nullable SqlValidatorNamespace getNamespace(SqlNode node) {
        switch (node.getKind()) {
            case COLLECTION_TABLE:
                return namespaces.get(node);
            default:
                return super.getNamespace(node);
        }
    }

    @Override
    protected void validateTableFunction(SqlCall node, SqlValidatorScope scope, RelDataType targetRowType) {
        validateQuery(node, scope, targetRowType);
    }

    @Override
    protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        if (node instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) node;
            if (sqlBasicCall.getOperator() instanceof SqlMapValueConstructor) {
                SqlMapValueConstructor mapValueConstructor = (SqlMapValueConstructor) sqlBasicCall.getOperator();
                if ("MAP".equalsIgnoreCase(mapValueConstructor.getName())) {
                    return;
                }
            }
        }
        super.inferUnknownTypes(inferredType, scope, node);
    }

}
