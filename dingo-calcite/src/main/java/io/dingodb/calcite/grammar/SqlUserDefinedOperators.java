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

package io.dingodb.calcite.grammar;

import io.dingodb.common.table.DiskAnnTable;
import io.dingodb.exec.fun.vector.VectorCosineDistanceFun;
import io.dingodb.exec.fun.vector.VectorIPDistanceFun;
import io.dingodb.exec.fun.vector.VectorL2DistanceFun;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.SqlCosineSimilarityOperator;
import org.apache.calcite.sql2rel.SqlDiskAnnOperator;
import org.apache.calcite.sql2rel.SqlDocumentOperator;
import org.apache.calcite.sql2rel.SqlFunctionScanOperator;
import org.apache.calcite.sql2rel.SqlHybridSearchOperator;
import org.apache.calcite.sql2rel.SqlIPDistanceOperator;
import org.apache.calcite.sql2rel.SqlL2DistanceOperator;
import org.apache.calcite.sql2rel.SqlLikeBinaryOperator;
import org.apache.calcite.sql2rel.SqlVectorOperator;

public class SqlUserDefinedOperators {

    public static SqlLikeBinaryOperator LIKE_BINARY =
        new SqlLikeBinaryOperator("LIKE_BINARY", SqlKind.OTHER_FUNCTION, false, true);

    public static SqlLikeBinaryOperator NOT_LIKE_BINARY =
        new SqlLikeBinaryOperator("NOT LIKE_BINARY", SqlKind.OTHER_FUNCTION, true, true);

    public static SqlFunctionScanOperator SCAN = new SqlFunctionScanOperator("SCAN", SqlKind.COLLECTION_TABLE);

    public static SqlVectorOperator VECTOR = new SqlVectorOperator("VECTOR", SqlKind.COLLECTION_TABLE);

    public static SqlDocumentOperator DOCUMENT = new SqlDocumentOperator("TEXT", SqlKind.COLLECTION_TABLE );

    public static SqlDocumentOperator TEXT_SEARCH = new SqlDocumentOperator("TEXT_SEARCH", SqlKind.COLLECTION_TABLE);

    public static SqlHybridSearchOperator HYBRID_SEARCH = new SqlHybridSearchOperator("HYBRID_SEARCH", SqlKind.COLLECTION_TABLE);

    public static SqlDiskAnnOperator DISK_ANN_BUILD = new SqlDiskAnnOperator(DiskAnnTable.TABLE_BUILD_NAME, SqlKind.COLLECTION_TABLE);

    public static SqlDiskAnnOperator DISK_ANN_LOAD = new SqlDiskAnnOperator(DiskAnnTable.TABLE_LOAD_NAME, SqlKind.COLLECTION_TABLE);

    public static SqlDiskAnnOperator DISK_ANN_STATUS = new SqlDiskAnnOperator(DiskAnnTable.TABLE_STATUS_NAME, SqlKind.COLLECTION_TABLE);

    public static SqlDiskAnnOperator DISK_ANN_COUNT_MEMORY = new SqlDiskAnnOperator(DiskAnnTable.TABLE_COUNT_MEMORY_NAME, SqlKind.COLLECTION_TABLE);

    public static SqlDiskAnnOperator DISK_ANN_RESET = new SqlDiskAnnOperator(DiskAnnTable.TABLE_RESET_NAME, SqlKind.COLLECTION_TABLE);

    public static SqlCosineSimilarityOperator COSINE_SIMILARITY
        = new SqlCosineSimilarityOperator(VectorCosineDistanceFun.NAME, SqlKind.OTHER_FUNCTION);

    public static SqlIPDistanceOperator IP_DISTANCE
        = new SqlIPDistanceOperator(VectorIPDistanceFun.NAME, SqlKind.OTHER_FUNCTION);

    public static SqlL2DistanceOperator L2_DISTANCE
        = new SqlL2DistanceOperator(VectorL2DistanceFun.NAME, SqlKind.OTHER_FUNCTION);



}
