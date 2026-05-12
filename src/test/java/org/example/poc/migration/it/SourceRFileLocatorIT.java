package org.example.poc.migration.it;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.example.poc.migration.data.DatasetGenerator;
import org.example.poc.migration.data.Event;
import org.example.poc.migration.env.EnvironmentSetup;
import org.example.poc.migration.locate.SourceRFileLocator;
import org.example.poc.migration.locate.SourceRFileLocator.RFileRef;
import org.example.poc.migration.transform.RFileIO;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase-6 integration test for {@link SourceRFileLocator}: after generating events into
 * {@code events_legacy} and forcing a compaction, the locator returns the resulting RFile
 * paths from {@code accumulo.metadata}, each file exists on the local FS, and re-reading it
 * via {@link RFileIO} yields the expected entry count.
 *
 * <p>Failsafe-named ({@code *IT.java}) so {@code mvn test} skips it — MiniAccumulo bootstrap
 * is in the 30–60s range. Run with {@code mvn verify}.
 *
 * <p>Adds two splits to {@code events_legacy} before writing so the row-range filter can be
 * meaningfully exercised: with all events in a single tablet, every filter would return
 * the same set and the test would be vacuous.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SourceRFileLocatorIT {

    private static final int TOTAL_EVENTS = 100;
    private static final int UNIQUE_USERS = 10;
    private static final int UNIQUE_SESSIONS = 25;
    private static final long TIME_START = 1_735_689_600_000L; // 2025-01-01
    private static final long TIME_END = 1_738_368_000_000L;   // 2025-02-01
    private static final long SEED = 42L;

    @TempDir
    static java.nio.file.Path tempDir;

    private EnvironmentSetup env;
    private SourceRFileLocator locator;
    private List<Event> generated;

    @BeforeAll
    void setUp() throws Exception {
        env = new EnvironmentSetup(tempDir.resolve("mini-accumulo"), "secret");
        env.start();
        env.createTables();

        // Split events_legacy so locating by row range can return a proper subset.
        // Source row format is "<userId>_<timestamp>"; the userIds are user-0..user-9 in this
        // run (uniqueUsers=10), so a split at "user-4" leaves users 0..3 in one tablet and
        // 4..9 in the next. Add a second split for a three-tablet layout.
        TreeSet<Text> sourceSplits = new TreeSet<>();
        sourceSplits.add(new Text("user-4"));
        sourceSplits.add(new Text("user-7"));
        env.applySplits(EnvironmentSetup.SOURCE_TABLE, sourceSplits);

        DatasetGenerator generator = new DatasetGenerator(
                TOTAL_EVENTS, UNIQUE_USERS, UNIQUE_SESSIONS, TIME_START, TIME_END, SEED);
        generated = generator.generate();
        generator.writeToLegacy(env.client(), EnvironmentSetup.SOURCE_TABLE, generated);

        locator = new SourceRFileLocator(env.client());
        locator.compactAndWait(EnvironmentSetup.SOURCE_TABLE);
    }

    @AfterAll
    void tearDown() throws Exception {
        if (env != null) {
            env.close();
        }
    }

    @Test
    void locate_returnsAllRFilesForTable() throws Exception {
        List<RFileRef> files = locator.locate(EnvironmentSetup.SOURCE_TABLE);
        assertFalse(files.isEmpty(),
                "locator must find at least one RFile after dataset write + compaction");

        // With 2 splits there are 3 tablets — each contributes at least one file after the
        // forced major compaction. (A perfectly empty tablet could contribute zero, but
        // generated userIds 0..9 cover all three tablets given the splits.)
        assertEquals(3, files.stream().map(RFileRef::tabletEndRow).distinct().count(),
                "expected one file group per tablet; tablet end-rows: "
                        + files.stream().map(RFileRef::tabletEndRow).toList());

        // Entry count across all files must equal the dataset size (no overlapping/duplicates
        // because we forced a major compaction).
        long totalEntries = files.stream().mapToLong(RFileRef::numEntries).sum();
        assertEquals(TOTAL_EVENTS, totalEntries,
                "sum of metadata entry counts must equal the number of source events");
    }

    @Test
    void locate_filesExistAndAreReadable() throws Exception {
        List<RFileRef> files = locator.locate(EnvironmentSetup.SOURCE_TABLE);
        Configuration hadoopConf = new Configuration();

        long roundTripEntries = 0;
        for (RFileRef ref : files) {
            Path p = new Path(ref.path());
            FileSystem fs = FileSystem.get(URI.create(ref.path()), hadoopConf);
            assertTrue(fs.exists(p), "RFile path from metadata does not exist on disk: " + ref.path());
            assertEquals(ref.sizeBytes(), fs.getFileStatus(p).getLen(),
                    "metadata-reported size disagrees with on-disk size for " + ref.path());

            // Reading the file via the static RFile API must yield exactly the entry count
            // recorded in the metadata for this file (sanity-check the read path that Phase-7
            // Spark workers will use).
            try (RFileIO.Reader reader = RFileIO.openReader(ref.path(), fs)) {
                long entries = 0;
                for (var ignored : reader) {
                    entries++;
                }
                assertEquals(ref.numEntries(), entries,
                        "round-trip entry count differs from metadata for " + ref.path());
                roundTripEntries += entries;
            }
        }
        assertEquals(TOTAL_EVENTS, roundTripEntries,
                "round-tripping all located RFiles must reproduce the full dataset");
    }

    @Test
    void locate_byRange_returnsTabletSubset() throws Exception {
        List<RFileRef> all = locator.locate(EnvironmentSetup.SOURCE_TABLE);

        // The middle tablet (split at user-4..user-7) covers rows with userIds 4..6. Asking
        // for rows in ("user-4", "user-7"] must return that tablet only — not the user-0..3
        // tablet nor the user-7..9 tablet.
        List<RFileRef> middle = locator.locate(
                EnvironmentSetup.SOURCE_TABLE,
                new Text("user-5_"), new Text("user-6_~"));

        assertFalse(middle.isEmpty(), "middle-range query must return at least one file");
        assertTrue(middle.size() < all.size(),
                "row-range filter must return a proper subset; full=" + all.size()
                        + " filtered=" + middle.size());
        assertEquals(1, middle.stream().map(RFileRef::tabletEndRow).distinct().count(),
                "middle-range query must touch exactly one tablet");
        assertEquals("user-7", middle.get(0).tabletEndRow(),
                "middle tablet's endRow must be user-7");
        assertEquals("user-4", middle.get(0).tabletPrevEndRow(),
                "middle tablet's prevEndRow must be user-4");
    }

    @Test
    void locate_byRange_unboundedEndIncludesDefaultTablet() throws Exception {
        // A right-unbounded request (endRow=null) must reach the final tablet whose
        // metadata row is `<tableId><` — exercises the null-tablet-end branch.
        List<RFileRef> tail = locator.locate(
                EnvironmentSetup.SOURCE_TABLE,
                new Text("user-8"), null);
        assertNotNull(tail, "tail query result must not be null");
        assertFalse(tail.isEmpty(), "tail query must include the default tablet");
        assertTrue(tail.stream().anyMatch(f -> f.tabletEndRow() == null),
                "tail query must include the default tablet (endRow == null)");
    }
}
