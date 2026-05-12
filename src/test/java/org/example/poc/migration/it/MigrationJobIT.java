package org.example.poc.migration.it;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.poc.migration.data.DatasetGenerator;
import org.example.poc.migration.data.Event;
import org.example.poc.migration.env.EnvironmentSetup;
import org.example.poc.migration.locate.SourceRFileLocator;
import org.example.poc.migration.locate.SourceRFileLocator.RFileRef;
import org.example.poc.migration.transform.EventTransformer;
import org.example.poc.migration.transform.MigrationJob;
import org.example.poc.migration.transform.RFileIO;
import org.example.poc.migration.transform.TransformResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase-7 integration test for {@link MigrationJob}: runs the Spark transformation against
 * source RFiles produced by {@link DatasetGenerator} on a real {@code MiniAccumuloCluster},
 * and asserts the staging RFiles satisfy the data-model and split-alignment contracts.
 *
 * <p>Heavy: starts MiniAccumulo + a {@code local[*]} {@link JavaSparkContext} per class, so
 * uses {@link TestInstance.Lifecycle#PER_CLASS} to amortize. Failsafe-named ({@code *IT}) so
 * {@code mvn test} skips it; run with {@code mvn verify}.
 *
 * <p>The 1→7 fan-out of {@link EventTransformer} is unit-tested already; here we verify the
 * end-to-end Spark wiring: read source RFiles → fan out → partition by target-table splits →
 * sort within partition → write one RFile per non-empty partition. Specifically:
 * <ul>
 *   <li>Per-table entry counts honor the 1/1/1/3/1 cardinality (CC-1 shape).</li>
 *   <li>RFiles are well-ordered (would have thrown at write time otherwise — UT-5 invariant).</li>
 *   <li>Each output RFile lies entirely within one tablet of the target table
 *       (architecture §5.2 alignment).</li>
 *   <li>Source-event timestamps are preserved on every produced KeyValue (architecture §5.4).</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MigrationJobIT {

    private static final int TOTAL_EVENTS = 100;
    private static final int UNIQUE_USERS = 10;
    private static final int UNIQUE_SESSIONS = 25;
    private static final long TIME_START = 1_735_689_600_000L;
    private static final long TIME_END = 1_738_368_000_000L;
    private static final long SEED = 42L;

    @TempDir
    static Path tempDir;

    private EnvironmentSetup env;
    private JavaSparkContext jsc;
    private MigrationJob job;
    private TransformResult result;

    private List<Event> generated;
    private Map<String, SortedSet<Text>> splitsByTable;
    private List<RFileRef> sourceFiles;
    private String stagingBaseDir;

    @BeforeAll
    void setUp() throws Exception {
        env = new EnvironmentSetup(tempDir.resolve("mini-accumulo"), "secret");
        env.start();
        env.createTables();

        DatasetGenerator generator = new DatasetGenerator(
                TOTAL_EVENTS, UNIQUE_USERS, UNIQUE_SESSIONS, TIME_START, TIME_END, SEED);
        generated = generator.generate();
        splitsByTable = generator.splitPoints(generated);
        for (Map.Entry<String, SortedSet<Text>> e : splitsByTable.entrySet()) {
            env.applySplits(e.getKey(), e.getValue());
        }
        generator.writeToLegacy(env.client(), EnvironmentSetup.SOURCE_TABLE, generated);

        SourceRFileLocator locator = new SourceRFileLocator(env.client());
        locator.compactAndWait(EnvironmentSetup.SOURCE_TABLE);
        sourceFiles = locator.locate(EnvironmentSetup.SOURCE_TABLE);

        stagingBaseDir = "file:" + tempDir.resolve("staging").toAbsolutePath();

        // Single shared SparkContext for the whole class — startup is the slowest piece.
        // local[2] (not local[*]) keeps the test deterministic and lighter on CI.
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("MigrationJobIT");
        jsc = new JavaSparkContext(sparkConf);

        job = new MigrationJob("local[2]", "MigrationJobIT");
        result = job.runWith(jsc, sourceFiles.stream().map(RFileRef::path).toList(),
                splitsByTable, stagingBaseDir);
    }

    @AfterAll
    void tearDown() throws Exception {
        if (jsc != null) {
            jsc.close();
        }
        if (env != null) {
            env.close();
        }
    }

    @Test
    void perTableEntryCounts_matchTheFanOutShape() {
        // CC-1 shape (test-plan §4.1): 1+1+1+3+1 = 7 entries per source event.
        assertEquals(TOTAL_EVENTS, result.entryCountsByTable().get(EventTransformer.EVENTS_BY_ID));
        assertEquals(TOTAL_EVENTS, result.entryCountsByTable().get(EventTransformer.EVENTS_BY_USER));
        assertEquals(TOTAL_EVENTS, result.entryCountsByTable().get(EventTransformer.EVENTS_BY_SESSION));
        assertEquals(3L * TOTAL_EVENTS,
                result.entryCountsByTable().get(EventTransformer.EVENT_COMPONENTS_SEARCHABLE));
        assertEquals(TOTAL_EVENTS, result.entryCountsByTable().get(EventTransformer.EVENT_STATS_BY_TYPE));
    }

    @Test
    void everyStagingRFile_existsAndIsReadable() throws Exception {
        Configuration hadoopConf = new Configuration();
        for (Map.Entry<String, List<String>> e : result.stagingPathsByTable().entrySet()) {
            assertFalse(e.getValue().isEmpty(),
                    "no staging files produced for " + e.getKey());
            for (String pathStr : e.getValue()) {
                FileSystem fs = FileSystem.get(URI.create(pathStr), hadoopConf);
                org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(pathStr);
                assertTrue(fs.exists(p),
                        "missing staging file " + pathStr + " for table " + e.getKey());
                assertTrue(fs.getFileStatus(p).getLen() > 0,
                        "staging file is zero bytes — RFile footer missing? " + pathStr);
            }
        }
    }

    @Test
    void perFileEntries_areKeyOrdered_andSumToTheReportedCount() throws Exception {
        // RFile.append throws on out-of-order keys (UT-5); the sortWithinPartitions invariant
        // is enforced at write time, so simply re-reading is the proof. We additionally check
        // monotonic order on the read side as defense-in-depth.
        Configuration hadoopConf = new Configuration();
        for (Map.Entry<String, List<String>> e : result.stagingPathsByTable().entrySet()) {
            long countByRead = 0;
            for (String pathStr : e.getValue()) {
                FileSystem fs = FileSystem.get(URI.create(pathStr), hadoopConf);
                Key prev = null;
                try (RFileIO.Reader reader = RFileIO.openReader(pathStr, fs)) {
                    for (Map.Entry<Key, Value> kv : reader) {
                        if (prev != null) {
                            assertTrue(prev.compareTo(kv.getKey()) <= 0,
                                    "out-of-order keys in " + pathStr
                                            + ": " + prev + " then " + kv.getKey());
                        }
                        prev = new Key(kv.getKey());
                        countByRead++;
                    }
                }
            }
            assertEquals(result.entryCountsByTable().get(e.getKey()).longValue(), countByRead,
                    "read-back count differs from reported count for table " + e.getKey());
        }
    }

    @Test
    void perFileRows_lieEntirelyWithinOneTablet_ofTheTargetTable() throws Exception {
        // Architecture §5.2: each produced RFile must fit inside one target tablet so bulk
        // import does not have to split on the fly. With the partitioner aligned to the table's
        // splits, the min and max row of each file must fall in the same tablet partition.
        Configuration hadoopConf = new Configuration();
        for (Map.Entry<String, List<String>> e : result.stagingPathsByTable().entrySet()) {
            String table = e.getKey();
            List<Text> splits = List.copyOf(splitsByTable.get(table));
            for (String pathStr : e.getValue()) {
                FileSystem fs = FileSystem.get(URI.create(pathStr), hadoopConf);
                Text minRow = null;
                Text maxRow = null;
                try (RFileIO.Reader reader = RFileIO.openReader(pathStr, fs)) {
                    for (Map.Entry<Key, Value> kv : reader) {
                        Text row = kv.getKey().getRow();
                        if (minRow == null) minRow = new Text(row);
                        maxRow = new Text(row);
                    }
                }
                assertNotNull(minRow, "empty RFile should not have been emitted: " + pathStr);
                int minPart = partitionOf(minRow, splits);
                int maxPart = partitionOf(maxRow, splits);
                assertEquals(minPart, maxPart,
                        "RFile " + pathStr + " spans tablets " + minPart + ".." + maxPart
                                + " (table " + table + " splits=" + splits + ")");
            }
        }
    }

    @Test
    void everyKey_carriesItsSourceTimestamp() throws Exception {
        // Architecture §5.4: produced KVs must carry the source KeyValue timestamp, never the
        // event.timestamp (a JSON field) or now(). DatasetGenerator writes the legacy table
        // with ingestionTs = TIME_END + i for i in [0, TOTAL_EVENTS), so every produced ts must
        // fall in [TIME_END, TIME_END + TOTAL_EVENTS).
        long minAllowed = TIME_END;
        long maxAllowed = TIME_END + TOTAL_EVENTS;

        Configuration hadoopConf = new Configuration();
        for (List<String> paths : result.stagingPathsByTable().values()) {
            for (String pathStr : paths) {
                FileSystem fs = FileSystem.get(URI.create(pathStr), hadoopConf);
                try (RFileIO.Reader reader = RFileIO.openReader(pathStr, fs)) {
                    for (Map.Entry<Key, Value> kv : reader) {
                        long ts = kv.getKey().getTimestamp();
                        assertTrue(ts >= minAllowed && ts < maxAllowed,
                                "key timestamp " + ts + " outside source-ingest range ["
                                        + minAllowed + ", " + maxAllowed + ") in " + pathStr);
                    }
                }
            }
        }
    }

    @Test
    void rerun_isIdempotent_overwritesStagingDirCleanly() throws Exception {
        // NFR-3 spirit: re-running the wave on the same input must produce the same per-table
        // entry counts. (We can't byte-compare RFiles trivially because partition counts may
        // hash differently if Spark assigns work nondeterministically — but counts and
        // per-table fan-out shape must match.)
        TransformResult second = job.runWith(jsc,
                sourceFiles.stream().map(RFileRef::path).toList(),
                splitsByTable, stagingBaseDir);

        Map<String, Long> first = result.entryCountsByTable();
        Map<String, Long> again = second.entryCountsByTable();
        assertEquals(new HashSet<>(first.keySet()), new HashSet<>(again.keySet()));
        for (String table : first.keySet()) {
            assertEquals(first.get(table), again.get(table),
                    "re-run produced different entry count for " + table);
        }

        // Reset cache so subsequent tests don't see the second run's paths.
        result = job.runWith(jsc, sourceFiles.stream().map(RFileRef::path).toList(),
                splitsByTable, stagingBaseDir);
    }

    private static int partitionOf(Text row, List<Text> splits) {
        // Mirror TargetTablePartitioner semantics — duplicated here on purpose so the test
        // does not depend on the production class's internals (would mask a sign bug).
        int idx = java.util.Collections.binarySearch(splits, row);
        return idx >= 0 ? idx : -(idx + 1);
    }

}
