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

package io.dingodb.server.executor.ddl;

import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.ddl.RecoverInfo;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.table.IndexDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public final class TableUtil {

    private TableUtil() {
    }

    public static Pair<TableDefinition, String> createTable(DdlJob ddlJob) {
        long schemaId = ddlJob.getSchemaId();
        TableDefinition tableInfo = (TableDefinition) ddlJob.getArgs().get(0);
        tableInfo.setSchemaState(SchemaState.SCHEMA_NONE);
        long tableId = ddlJob.getTableId();
        tableInfo.setPrepareTableId(tableId);

        if (tableInfo.getSchemaState() == SchemaState.SCHEMA_NONE) {
            tableInfo.setSchemaState(SchemaState.SCHEMA_PUBLIC);
            MetaService metaService = MetaService.root();
            List<IndexDefinition> indices = tableInfo.getIndices();
            if (indices != null) {
                indices.forEach(index -> index.setSchemaState(SchemaState.SCHEMA_PUBLIC));
            }
            try {
                assert indices != null;
                metaService.createTables(schemaId, tableInfo, indices);
                return Pair.of(tableInfo, null);
            } catch (Exception e) {
                LogUtils.error(log, "[ddl-error]" + e.getMessage(), e);
                if (e.getMessage() != null && e.getMessage().contains("table has existed")) {
                    ddlJob.setState(JobState.jobStateCancelled);
                    return Pair.of(null, "table has existed");
                }
                metaService.rollbackCreateTable(schemaId, tableInfo, indices);
                ddlJob.setState(JobState.jobStateCancelled);
                if (e instanceof NullPointerException) {
                    return Pair.of(null, "NullPointerException");
                }
                return Pair.of(null, e.getMessage());
            }
        }
        return Pair.of(tableInfo, "ErrInvalidDDLState");
    }

    public static Pair<TableDefinition, String> createView(DdlJob ddlJob) {
        long schemaId = ddlJob.getSchemaId();
        TableDefinition tableInfo = (TableDefinition) ddlJob.getArgs().get(0);
        tableInfo.setSchemaState(SchemaState.SCHEMA_NONE);
        long tableId = ddlJob.getTableId();
        tableInfo.setPrepareTableId(tableId);

        InfoSchemaService service = InfoSchemaService.root();
        Object tabObj = service.getTable(schemaId, tableInfo.getName());
        if (tabObj != null) {
            ddlJob.setState(JobState.jobStateCancelled);
            return Pair.of(null, "view has existed");
        }
        if (tableInfo.getSchemaState() == SchemaState.SCHEMA_NONE) {
            tableInfo.setSchemaState(SchemaState.SCHEMA_PUBLIC);
            MetaService metaService = MetaService.root();
            List<IndexDefinition> indices = tableInfo.getIndices();
            if (indices != null) {
                indices.forEach(index -> index.setSchemaState(SchemaState.SCHEMA_PUBLIC));
            }
            try {
                assert indices != null;
                metaService.createView(schemaId, tableInfo.getName(), tableInfo);
                return Pair.of(tableInfo, null);
            } catch (Exception e) {
                metaService.rollbackCreateTable(schemaId, tableInfo, indices);
                LogUtils.error(log, "[ddl-error]" + e.getMessage(), e);
                ddlJob.setState(JobState.jobStateCancelled);
                if (e instanceof NullPointerException) {
                    return Pair.of(null, "NullPointerException");
                }
                return Pair.of(null, e.getMessage());
            }
        }
        return Pair.of(tableInfo, "ErrInvalidDDLState");
    }

    public static Pair<Long, String> updateVersionAndTableInfos(DdlContext dc,
                                                                DdlJob job,
                                                                TableDefinitionWithId tableInfo,
                                                                boolean shouldUpdateVer) {
        Long version = 0L;
        updateTable(job.getSchemaId(), tableInfo);
        if (shouldUpdateVer) {
            Pair<Long, String> res = DdlWorker.updateSchemaVersion(dc, job);
            if (res.getValue() != null) {
                return res;
            }
            version = res.getKey();
        }
        return Pair.of(version, null);
    }

    public static Pair<Long, String> updateVersionAndIndexInfos(
        DdlContext dc,
        DdlJob job,
        TableDefinitionWithId indexInfo,
        boolean shouldUpdateVer
    ) {
        Long version = 0L;
        updateIndex(indexInfo);
        if (shouldUpdateVer) {
            Pair<Long, String> res = DdlWorker.updateSchemaVersion(dc, job);
            if (res.getValue() != null) {
                return res;
            }
            version = res.getKey();
        }
        return Pair.of(version, null);
    }

    public static void updateTable(long schemaId, TableDefinitionWithId tableInfo) {
        // to set startTs
        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        infoSchemaService.updateTable(schemaId, tableInfo);
    }

    public static void updateReplicaTable(long schemaId, long tableId, TableDefinitionWithId tableInfo) {
        InfoSchemaService.root().updateReplicaTable(schemaId, tableId, tableInfo);
    }

    public static void updateIndex(TableDefinitionWithId indexInfo) {
        long tableId = indexInfo.getTableId().getParentEntityId();

        InfoSchemaService infoSchemaService = InfoSchemaService.root();
        infoSchemaService.updateIndex(tableId, indexInfo);
    }

    public static Pair<TableDefinitionWithId, String> getTableInfoAndCancelFaultJob(DdlJob ddlJob, long schemaId) {
        Pair<TableDefinitionWithId, String> res = DdlWorker.checkTableExistAndCancelNonExistJob(ddlJob, schemaId);
        if (res.getValue() != null || res.getKey() == null) {
            return res;
        }
        if (res.getKey().getTableDefinition().getSchemaState()
              != io.dingodb.sdk.service.entity.common.SchemaState.SCHEMA_PUBLIC) {
            ddlJob.setState(JobState.jobStateCancelled);
            return Pair.of(null, "table is not in public");
        }
        return res;
    }

    public static void recoverTable(
        DdlJob job,
        RecoverInfo recoverInfo,
        TableDefinitionWithId tableDefinitionWithId,
        List<Object> indexList
    ) {
        // remove gc_delete_range to gc_delete_range_done
        String sql = "select region_id,start_key,end_key,job_id,ts, element_id, element_type from mysql.gc_delete_range where ts<"
            + recoverInfo.getDropJobId();
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            List<Object[]> gcResults = session.executeQuery(sql);
            LogUtils.info(log, "gcDeleteRange result size: {}, safePointTs:{}",
                gcResults.size(), ExecutionEnvironment.INSTANCE);
            gcResults.forEach(objects -> {
                long regionId = (long) objects[0];
                try {
                    long jobId = (long) objects[3];
                    long ts = (long) objects[4];
                    String startKey = objects[1].toString();
                    String endKey = objects[2].toString();
                    String eleId = objects[5].toString();
                    String eleType = objects[6].toString();
                    if (!JobTableUtil.gcDeleteDone(jobId, ts, regionId, startKey, endKey, eleId, eleType)) {
                        LogUtils.error(log, "remove gcDeleteTask failed");
                    }
                } catch (Exception e) {
                    LogUtils.error(log, "gcDeleteRange error, regionId:{}", regionId, e);
                }
            });
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }

        // create table Info and set autoIncId
        MetaService.root().recoverTable(job.getSchemaId(), tableDefinitionWithId, indexList);
    }
}
