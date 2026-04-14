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

package io.dingodb.exec.operator.spill;

import io.dingodb.common.ExecutionContext;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.time.DingoTimeZoneContext;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.impl.JobManagerImpl;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.data.SortDirection;
import io.dingodb.exec.operator.data.SortNullDirection;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.aggregate.AggCache;
import io.dingodb.exec.aggregate.CountAllAgg;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.params.AggregateParams;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.operator.params.SortParam;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.tso.TsoService;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the spill-to-disk infrastructure:
 * <ul>
 *   <li>{@link SpillManager} – file lifecycle</li>
 *   <li>{@link TupleSpillFile} – write / lazy-read round-trip</li>
 *   <li>{@link SortParam} – spill batch accumulation</li>
 * </ul>
 */
public class TestSpillOperator {

    private static final JobManager jobManager = JobManagerImpl.INSTANCE;

    private static final DingoType INT_STRING_DOUBLE =
        DingoTypeFactory.INSTANCE.tuple("INT", "STRING", "DOUBLE");

    @BeforeEach
    @SneakyThrows
    public void before() {
        DingoTimeZoneContext.setTimeZone(TimeZone.getDefault());
        // DingoConfiguration.parse(TestSpillOperator.class.getResource("/executor.yaml").getPath());
        DingoConfiguration.instance().getSpill().setDir("/tmp/dingo-spill");
    }

    @AfterEach
    public void cleanupSpills() {
        SpillManager.INSTANCE.cleanupAll();
    }

    // -------------------------------------------------------------------------
    // SpillManager tests
    // -------------------------------------------------------------------------

    @Test
    public void testSpillManagerCreatesAndDeletesFiles() throws IOException {
        int before = SpillManager.INSTANCE.getActiveFileCount();
        File f = SpillManager.INSTANCE.createSpillFile("testJob", "testOp");
        assertThat(f).exists();
        assertThat(SpillManager.INSTANCE.getActiveFileCount()).isEqualTo(before + 1);

        SpillManager.INSTANCE.deleteSpillFile(f);
        assertThat(f).doesNotExist();
        assertThat(SpillManager.INSTANCE.getActiveFileCount()).isEqualTo(before);
    }

    @Test
    public void testSpillManagerDeleteNullIsNoOp() {
        // Should not throw
        SpillManager.INSTANCE.deleteSpillFile(null);
    }

    // -------------------------------------------------------------------------
    // TupleSpillFile tests
    // -------------------------------------------------------------------------

    @Test
    public void testTupleSpillFileRoundTrip() throws IOException {
        List<Object[]> tuples = Arrays.asList(
            new Object[]{1, "Alice", 3.5},
            new Object[]{2, "Betty", 3.6},
            new Object[]{3, "Cindy", 3.7}
        );

        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile("testJob", "testOp"), INT_STRING_DOUBLE
        );
        sf.write(tuples);
        sf.finishWrite();

        assertThat(sf.getTupleCount()).isEqualTo(3);

        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> it = sf.iterator();
        while (it.hasNext()) {
            result.add(it.next());
        }
        sf.close();

        assertThat(result).hasSize(3);
        for (int i = 0; i < tuples.size(); i++) {
            assertThat(result.get(i)).containsExactly(tuples.get(i));
        }
    }

    @Test
    public void testTupleSpillFileEmptyWrite() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile("testJob", "testOp"), INT_STRING_DOUBLE
        );
        sf.write(Collections.emptyList());
        sf.finishWrite();

        assertThat(sf.getTupleCount()).isEqualTo(0);

        Iterator<Object[]> it = sf.iterator();
        assertThat(it.hasNext()).isFalse();
        sf.close();
    }

    @Test
    public void testTupleSpillFileMultiBatchWrite() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile("testJob", "testOp"), INT_STRING_DOUBLE
        );
        // Write two batches sequentially
        sf.write(Arrays.asList(
            new Object[]{1, "A", 1.0},
            new Object[]{2, "B", 2.0}
        ));
        sf.write(Arrays.asList(
            new Object[]{3, "C", 3.0},
            new Object[]{4, "D", 4.0}
        ));
        sf.finishWrite();

        assertThat(sf.getTupleCount()).isEqualTo(4);

        List<Object[]> result = new ArrayList<>();
        sf.iterator().forEachRemaining(result::add);
        sf.close();

        assertThat(result).hasSize(4);
        assertThat((Integer) result.get(0)[0]).isEqualTo(1);
        assertThat((Integer) result.get(3)[0]).isEqualTo(4);
    }

    @Test
    public void testTupleSpillFileCloseDeletesFile() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile("testJob", "testOp"), INT_STRING_DOUBLE
        );
        File file = sf.getFile();
        assertThat(file).exists();
        sf.close();
        assertThat(file).doesNotExist();
    }

    @Test
    public void testTupleSpillFileBytesTracking() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile("testJob", "testOp"), INT_STRING_DOUBLE
        );
        assertThat(sf.getBytesWritten()).isEqualTo(0);

        sf.write(Arrays.asList(
            new Object[]{1, "Alice", 3.5},
            new Object[]{2, "Betty", 3.6}
        ));
        sf.finishWrite();

        assertThat(sf.getBytesWritten()).isGreaterThan(0);
        assertThat(sf.getFile().length()).isEqualTo(sf.getBytesWritten());

        sf.close();
    }

    @Test
    public void testSpillManagerTotalBytesSpilled() throws IOException {
        long bytesBefore = SpillManager.INSTANCE.getTotalBytesSpilled();

        TupleSpillFile sf1 = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile("testJob", "testOp"), INT_STRING_DOUBLE
        );
        sf1.write(Arrays.asList(
            new Object[]{1, "Alice", 3.5},
            new Object[]{2, "Betty", 3.6}
        ));
        sf1.finishWrite();
        long bytes1 = sf1.getBytesWritten();
        sf1.close();

        TupleSpillFile sf2 = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile("testJob", "testOp"), INT_STRING_DOUBLE
        );
        sf2.write(Collections.singletonList(
            new Object[]{3, "Cindy", 3.7}
        ));
        sf2.finishWrite();
        long bytes2 = sf2.getBytesWritten();
        sf2.close();

        long bytesAfter = SpillManager.INSTANCE.getTotalBytesSpilled();
        assertThat(bytesAfter - bytesBefore).isEqualTo(bytes1 + bytes2);
    }

    @Test
    public void testTupleSpillFileCannotWriteAfterFinish() throws IOException {
        TupleSpillFile sf = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile("testJob", "testOp"), INT_STRING_DOUBLE
        );
        sf.finishWrite();
        assertThatThrownBy(() -> sf.write(Collections.singletonList(new Object[]{1, "X", 0.0})))
            .isInstanceOf(IllegalStateException.class);
        sf.close();
    }

    // -------------------------------------------------------------------------
    // SortParam spill batch tests
    // -------------------------------------------------------------------------

    @Test
    public void testSortParamSpillBatch() throws IOException {
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        SortParam param = new SortParam(
            Collections.emptyList(), -1, 0, false, job.getExecutionContext(), INT_STRING_DOUBLE, 3
        );
        // Simulate init (normally called by Vertex.init())
        param.getCache().add(new Object[]{3, "C", 3.0});
        param.getCache().add(new Object[]{1, "A", 1.0});
        param.getCache().add(new Object[]{2, "B", 2.0});

        assertThat(param.isSpillEnabled()).isTrue();
        assertThat(param.hasSpillFiles()).isFalse();

        param.spillCurrentBatch();

        assertThat(param.getCache()).isEmpty();
        assertThat(param.getSpilledCount()).isEqualTo(3);
        assertThat(param.hasSpillFiles()).isTrue();
        assertThat(param.getSpillFiles()).hasSize(1);

        // Read back tuples from the spill file
        List<Object[]> spilled = new ArrayList<>();
        param.getSpillFiles().get(0).iterator().forEachRemaining(spilled::add);
        assertThat(spilled).hasSize(3);

        param.clear();
        assertThat(param.hasSpillFiles()).isFalse();
        assertThat(param.getSpilledCount()).isEqualTo(0);
    }

    @Test
    public void testSortParamSpillBatchWithComparator() throws IOException {
        // Spill with a sort order: ascending on column 0 (INT)
        SortCollation col0Asc = new SortCollation(0, SortDirection.ASCENDING, SortNullDirection.LAST);
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        SortParam param = new SortParam(
            Collections.singletonList(col0Asc), -1, 0, false, job.getExecutionContext(), INT_STRING_DOUBLE, 3
        );
        param.getCache().add(new Object[]{3, "C", 3.0});
        param.getCache().add(new Object[]{1, "A", 1.0});
        param.getCache().add(new Object[]{2, "B", 2.0});

        param.spillCurrentBatch();

        // The spill file should contain sorted tuples
        List<Object[]> spilled = new ArrayList<>();
        param.getSpillFiles().get(0).iterator().forEachRemaining(spilled::add);
        assertThat(spilled).hasSize(3);
        assertThat((Integer) spilled.get(0)[0]).isEqualTo(1);
        assertThat((Integer) spilled.get(1)[0]).isEqualTo(2);
        assertThat((Integer) spilled.get(2)[0]).isEqualTo(3);

        param.clear();
    }

    @Test
    public void testSortParamSpillDisabledWhenNoSchema() {
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        SortParam param = new SortParam(
            Collections.emptyList(), -1, 0, false, job.getExecutionContext()
        );
        assertThat(param.isSpillEnabled()).isFalse();
        assertThat(param.hasSpillFiles()).isFalse();
    }

    @Test
    public void testSortParamMultipleSpillBatches() throws IOException {
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        SortParam param = new SortParam(
            Collections.emptyList(), -1, 0, false, job.getExecutionContext(), INT_STRING_DOUBLE, 2
        );
        // First batch
        param.getCache().add(new Object[]{1, "A", 1.0});
        param.getCache().add(new Object[]{2, "B", 2.0});
        param.spillCurrentBatch();
        assertThat(param.getSpilledCount()).isEqualTo(2);

        // Second batch
        param.getCache().add(new Object[]{3, "C", 3.0});
        param.getCache().add(new Object[]{4, "D", 4.0});
        param.spillCurrentBatch();
        assertThat(param.getSpilledCount()).isEqualTo(4);

        assertThat(param.getSpillFiles()).hasSize(2);

        // Collect all spilled tuples
        List<Object[]> all = new ArrayList<>();
        for (TupleSpillFile sf : param.getSpillFiles()) {
            sf.iterator().forEachRemaining(all::add);
        }
        assertThat(all).hasSize(4);

        param.clear();
    }

    // -------------------------------------------------------------------------
    // HashJoinParam spill tests
    // -------------------------------------------------------------------------

    /** Left schema: (INT id, STRING name) */
    private static final DingoType LEFT_SCHEMA =
        DingoTypeFactory.INSTANCE.tuple("INT", "STRING");

    /** Right schema: (INT id, DOUBLE score) */
    private static final DingoType RIGHT_SCHEMA =
        DingoTypeFactory.INSTANCE.tuple("INT", "DOUBLE");

    /**
     * Creates a HashJoinParam configured for spill testing.
     * Join key: column 0 on both sides (INT id).
     * Left tuple: (INT id, STRING name), Right tuple: (INT id, DOUBLE score).
     */
    private HashJoinParam createTestHashJoinParam() {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setInnerSql(true); // skip memory pool creation in init()
        HashJoinParam param = new HashJoinParam(
            TupleMapping.of(new int[]{0}),  // leftMapping: join on col 0
            TupleMapping.of(new int[]{0}),  // rightMapping: join on col 0
            2,   // leftLength
            2,   // rightLength
            false, // leftRequired (inner join)
            false, // rightRequired (inner join)
            ctx
        );
        param.setJoinType("inner");
        param.setLeftSchema(LEFT_SCHEMA);
        param.setRightSchema(RIGHT_SCHEMA);
        return param;
    }

    @Test
    public void testHashJoinSpillDisabledWhenNoSchema() {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setInnerSql(true);
        HashJoinParam param = new HashJoinParam(
            TupleMapping.of(new int[]{0}), TupleMapping.of(new int[]{0}),
            2, 2, false, false, ctx
        );
        // Don't set leftSchema/rightSchema
        assertThat(param.isSpillEnabled()).isFalse();
        assertThat(param.hasSpilledPartitions()).isFalse();
    }

    @Test
    public void testHashJoinSpillPartitionsWritesAndClearsHashMap() throws IOException {
        HashJoinParam param = createTestHashJoinParam();
        // Manually initialize spill arrays (normally done in init())
        param.init(null);

        assertThat(param.isSpillEnabled()).isTrue();
        assertThat(param.hasSpilledPartitions()).isFalse();

        // Populate hashMap with right-side tuples (simulating Build phase)
        addRightTuple(param, 1, 95.0);
        addRightTuple(param, 2, 88.0);
        addRightTuple(param, 3, 76.5);
        addRightTuple(param, 1, 91.0); // duplicate key

        assertThat(param.getHashMap()).hasSize(3); // 3 distinct keys

        // Trigger spill
        param.spillPartitions();

        // hashMap should be empty after spill
        assertThat(param.getHashMap()).isEmpty();
        assertThat(param.hasSpilledPartitions()).isTrue();

        // Verify right spill files contain the data
        int totalSpilled = 0;
        for (int p = 0; p < HashJoinParam.NUM_PARTITIONS; p++) {
            if (param.isPartitionSpilled(p) && param.getRightSpillFiles()[p] != null) {
                param.getRightSpillFiles()[p].finishWrite();
                Iterator<Object[]> it = param.getRightSpillFiles()[p].iterator();
                while (it.hasNext()) {
                    Object[] tuple = it.next();
                    assertThat(tuple).hasSize(2); // rightLength = 2
                    totalSpilled++;
                }
            }
        }
        // 4 tuples total (key=1 has 2 tuples)
        assertThat(totalSpilled).isEqualTo(4);

        param.clear();
    }

    @Test
    public void testHashJoinSpillRightTupleAppendsToSpilledPartition() throws IOException {
        HashJoinParam param = createTestHashJoinParam();
        param.init(null);

        // Add initial tuples and spill
        addRightTuple(param, 1, 95.0);
        addRightTuple(param, 2, 88.0);
        param.spillPartitions();
        assertThat(param.getHashMap()).isEmpty();

        // Append more right tuples to already-spilled partitions
        Object[] lateTuple = new Object[]{1, 99.0};
        TupleKey key = new TupleKey(new Object[]{1});
        int partition = param.partitionOf(HashJoinParam.rtrimTupleKey(key));
        assertThat(param.isPartitionSpilled(partition)).isTrue();

        param.spillRightTuple(partition, lateTuple);

        // Finalize and read back
        param.finishRightSpillFiles();
        List<Object[]> tuples = new ArrayList<>();
        param.getRightSpillFiles()[partition].iterator().forEachRemaining(tuples::add);

        // Should have original tuple(s) for key=1 plus the late tuple
        boolean foundLate = false;
        for (Object[] t : tuples) {
            if ((Integer) t[0] == 1 && (Double) t[1] == 99.0) {
                foundLate = true;
            }
        }
        assertThat(foundLate).isTrue();

        param.clear();
    }

    @Test
    public void testHashJoinSpillLeftTuple() throws IOException {
        HashJoinParam param = createTestHashJoinParam();
        param.init(null);

        // Spill right side first to establish spilled partitions
        addRightTuple(param, 1, 95.0);
        param.spillPartitions();

        // Determine which partition key=1 maps to
        TupleKey key = new TupleKey(new Object[]{1});
        int partition = param.partitionOf(HashJoinParam.rtrimTupleKey(key));
        assertThat(param.isPartitionSpilled(partition)).isTrue();

        // Write left tuples for this partition
        param.spillLeftTuple(partition, new Object[]{1, "Alice"});
        param.spillLeftTuple(partition, new Object[]{1, "Bob"});

        // Read back
        assertThat(param.getLeftSpillFiles()[partition]).isNotNull();
        param.getLeftSpillFiles()[partition].finishWrite();
        List<Object[]> leftTuples = new ArrayList<>();
        param.getLeftSpillFiles()[partition].iterator().forEachRemaining(leftTuples::add);

        assertThat(leftTuples).hasSize(2);
        assertThat(leftTuples.get(0)).containsExactly(1, "Alice");
        assertThat(leftTuples.get(1)).containsExactly(1, "Bob");

        param.clear();
    }

    @Test
    public void testHashJoinSpillEndToEndInnerJoin() throws IOException {
        HashJoinParam param = createTestHashJoinParam();
        param.init(null);

        // ---- Build phase: add right tuples, then spill ----
        addRightTuple(param, 1, 95.0);
        addRightTuple(param, 2, 88.0);
        addRightTuple(param, 3, 76.5);
        param.spillPartitions();
        assertThat(param.getHashMap()).isEmpty();

        // Add a late right tuple for key=2
        TupleKey key2 = new TupleKey(new Object[]{2});
        int p2 = param.partitionOf(HashJoinParam.rtrimTupleKey(key2));
        param.spillRightTuple(p2, new Object[]{2, 92.0});

        // Finalize right spill files
        param.finishRightSpillFiles();

        // ---- Probe phase: write left tuples to spilled partitions ----
        Object[][] leftTuples = {
            {1, "Alice"}, {2, "Bob"}, {3, "Cindy"}, {4, "Dave"}
        };
        for (Object[] lt : leftTuples) {
            TupleKey lk = HashJoinParam.rtrimTupleKey(
                new TupleKey(param.getLeftMapping().revMap(lt)));
            int lp = param.partitionOf(lk);
            if (param.isPartitionSpilled(lp)) {
                param.spillLeftTuple(lp, lt);
            }
            // key=4 has no matching right tuple and its partition may or may not be spilled
        }

        // ---- Process spilled partitions (simulating what processSpilledPartitions does) ----
        List<Object[]> joinResults = new ArrayList<>();
        TupleMapping rightMapping = param.getRightMapping();
        TupleMapping leftMapping = param.getLeftMapping();

        for (int p = 0; p < HashJoinParam.NUM_PARTITIONS; p++) {
            if (!param.isPartitionSpilled(p)) continue;

            TupleSpillFile leftSf = param.getLeftSpillFiles()[p];
            TupleSpillFile rightSf = param.getRightSpillFiles()[p];

            if (leftSf != null) {
                leftSf.finishWrite();
            }

            // Load right into temp map
            java.util.Map<TupleKey, List<Object[]>> tempMap = new java.util.HashMap<>();
            if (rightSf != null) {
                Iterator<Object[]> rightIter = rightSf.iterator();
                while (rightIter.hasNext()) {
                    Object[] rt = rightIter.next();
                    TupleKey rk = HashJoinParam.rtrimTupleKey(
                        new TupleKey(rightMapping.revMap(rt)));
                    tempMap.computeIfAbsent(rk, k -> new ArrayList<>()).add(rt);
                }
            }

            // Probe left against temp map
            if (leftSf != null) {
                Iterator<Object[]> leftIter = leftSf.iterator();
                while (leftIter.hasNext()) {
                    Object[] lt = leftIter.next();
                    TupleKey lk = HashJoinParam.rtrimTupleKey(
                        new TupleKey(leftMapping.revMap(lt)));
                    List<Object[]> matched = tempMap.get(lk);
                    if (matched != null) {
                        for (Object[] rt : matched) {
                            Object[] combined = Arrays.copyOf(lt, 4);
                            System.arraycopy(rt, 0, combined, 2, 2);
                            joinResults.add(combined);
                        }
                    }
                }
            }
        }

        // ---- Verify join results ----
        // Expected matches (inner join):
        //   (1, "Alice") x (1, 95.0) → [1, "Alice", 1, 95.0]
        //   (2, "Bob") x (2, 88.0) → [2, "Bob", 2, 88.0]
        //   (2, "Bob") x (2, 92.0) → [2, "Bob", 2, 92.0]
        //   (3, "Cindy") x (3, 76.5) → [3, "Cindy", 3, 76.5]
        //   (4, "Dave") has no match → not in result
        // Total: 4 result rows

        assertThat(joinResults).hasSize(4);

        // Verify key=1 match
        long key1Matches = joinResults.stream()
            .filter(t -> (Integer) t[0] == 1 && "Alice".equals(t[1]))
            .count();
        assertThat(key1Matches).isEqualTo(1);

        // Verify key=2 matches (two right tuples: 88.0 and 92.0)
        long key2Matches = joinResults.stream()
            .filter(t -> (Integer) t[0] == 2 && "Bob".equals(t[1]))
            .count();
        assertThat(key2Matches).isEqualTo(2);

        // Verify key=3 match
        long key3Matches = joinResults.stream()
            .filter(t -> (Integer) t[0] == 3 && "Cindy".equals(t[1]))
            .count();
        assertThat(key3Matches).isEqualTo(1);

        param.clear();
    }

    @Test
    public void testHashJoinClearDeletesAllSpillFiles() throws IOException {
        HashJoinParam param = createTestHashJoinParam();
        param.init(null);

        addRightTuple(param, 1, 95.0);
        addRightTuple(param, 2, 88.0);
        param.spillPartitions();
        param.finishRightSpillFiles();

        // Collect spill file references before clear
        List<File> spillFiles = new ArrayList<>();
        for (TupleSpillFile sf : param.getRightSpillFiles()) {
            if (sf != null) spillFiles.add(sf.getFile());
        }
        assertThat(spillFiles).isNotEmpty();
        for (File f : spillFiles) {
            assertThat(f).exists();
        }

        param.clear();

        // All files should be deleted
        for (File f : spillFiles) {
            assertThat(f).doesNotExist();
        }
    }

    // -------------------------------------------------------------------------
    // HashJoin test helpers
    // -------------------------------------------------------------------------

    private static void addRightTuple(HashJoinParam param, int id, double score) {
        Object[] tuple = new Object[]{id, score};
        TupleKey key = HashJoinParam.rtrimTupleKey(
            new TupleKey(param.getRightMapping().revMap(tuple)));
        List<TupleWithJoinFlag> list = param.getHashMap()
            .computeIfAbsent(key, k -> Collections.synchronizedList(new LinkedList<>()));
        list.add(new TupleWithJoinFlag(tuple));
    }

    // -------------------------------------------------------------------------
    // AggregateParams spill tests
    // -------------------------------------------------------------------------

    /** Output schema for aggregate: (INT groupKey, LONG count). */
    private static final DingoType AGG_SCHEMA =
        DingoTypeFactory.INSTANCE.tuple("INT", "LONG");

    /**
     * Creates an AggregateParams with transient spill fields initialized for testing.
     * Bypasses {@code init(Vertex)} which requires a full DAG context.
     */
    private AggregateParams createTestAggregateParams(DingoType schema, int spillThreshold)
        throws Exception {
        TupleMapping keyMapping = TupleMapping.of(new int[]{0});
        List<Agg> aggList = Collections.singletonList(new CountAllAgg());
        AggregateParams param = new AggregateParams(keyMapping, aggList, schema, spillThreshold);
        if (schema != null) {
            setField(param, "cache", new AggCache(keyMapping, aggList));
            setField(param, "spillFiles", new ArrayList<TupleSpillFile>());
            setField(param, "spilledCount", 0L);
            setField(param, "jobId", "testJob");
            setField(param, "operatorId", "testOp");
        }
        return param;
    }

    private static void setField(Object obj, String name, Object value) throws Exception {
        Field f = obj.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(obj, value);
    }

    @SuppressWarnings("unchecked")
    private static List<TupleSpillFile> getSpillFiles(AggregateParams param) throws Exception {
        Field f = AggregateParams.class.getDeclaredField("spillFiles");
        f.setAccessible(true);
        return (List<TupleSpillFile>) f.get(param);
    }

    private static long getSpilledCount(AggregateParams param) throws Exception {
        Field f = AggregateParams.class.getDeclaredField("spilledCount");
        f.setAccessible(true);
        return (long) f.get(param);
    }

    @Test
    public void testAggregateParamSpillAggCache() throws Exception {
        AggregateParams param = createTestAggregateParams(AGG_SCHEMA, 100);
        assertThat(param.isSpillEnabled()).isTrue();

        // Add tuples: 3 distinct keys, key=1 appears twice
        param.getCache().addTuple(new Object[]{1, "Alice", 3.5});
        param.getCache().addTuple(new Object[]{2, "Betty", 3.6});
        param.getCache().addTuple(new Object[]{1, "Alice2", 3.8});
        param.getCache().addTuple(new Object[]{3, "Cindy", 3.7});
        assertThat(param.getCache().size()).isEqualTo(3);

        param.spillAggCache();

        assertThat(param.getCache().size()).isEqualTo(0);
        assertThat(getSpilledCount(param)).isEqualTo(3);
        assertThat(getSpillFiles(param)).hasSize(1);

        // Read back and verify: each entry is [key, countValue]
        List<Object[]> spilled = new ArrayList<>();
        getSpillFiles(param).get(0).iterator().forEachRemaining(spilled::add);
        assertThat(spilled).hasSize(3);

        // Key=1 was added twice, so its count should be 2
        Object[] key1Entry = spilled.stream()
            .filter(t -> (Integer) t[0] == 1).findFirst().orElse(null);
        assertThat(key1Entry).isNotNull();
        assertThat((Long) key1Entry[1]).isEqualTo(2L);

        param.clear();
    }

    @Test
    public void testAggregateParamPrepareResultsMergesSpilledData() throws Exception {
        AggregateParams param = createTestAggregateParams(AGG_SCHEMA, 100);

        // First batch: keys 1, 2
        param.getCache().addTuple(new Object[]{1, "A", 1.0});
        param.getCache().addTuple(new Object[]{2, "B", 2.0});
        param.getCache().addTuple(new Object[]{1, "A2", 1.1});
        param.spillAggCache();
        assertThat(param.getCache().size()).isEqualTo(0);

        // Second batch: keys 2, 3
        param.getCache().addTuple(new Object[]{2, "B2", 2.1});
        param.getCache().addTuple(new Object[]{3, "C", 3.0});
        param.getCache().addTuple(new Object[]{3, "C2", 3.1});
        // Don't spill the second batch — it stays in cache

        assertThat(getSpillFiles(param)).hasSize(1);
        assertThat(param.getCache().size()).isEqualTo(2);

        // prepareResults merges spilled data back into cache
        param.prepareResults();

        assertThat(getSpillFiles(param)).isEmpty();
        assertThat(getSpilledCount(param)).isEqualTo(0);

        // Collect final results from cache
        List<Object[]> results = new ArrayList<>();
        param.getCache().iterator().forEachRemaining(results::add);

        // 3 distinct keys: 1, 2, 3
        assertThat(results).hasSize(3);

        // key=1: count=2 (from spill), key=2: count=1 (spill) merged with count=1 (cache) = 2,
        // key=3: count=2 (cache only)
        for (Object[] row : results) {
            int key = (Integer) row[0];
            long count = (Long) row[1];
            assertThat(count).as("count for key=%d", key).isEqualTo(2L);
        }

        param.clear();
    }

    @Test
    public void testAggregateParamSpillDisabledWhenNoSchema() {
        AggregateParams param = new AggregateParams(
            TupleMapping.of(new int[]{0}),
            Collections.singletonList(new CountAllAgg())
        );
        assertThat(param.isSpillEnabled()).isFalse();
    }
}
