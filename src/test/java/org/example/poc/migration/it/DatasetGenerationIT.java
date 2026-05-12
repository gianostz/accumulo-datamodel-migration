package org.example.poc.migration.it;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.example.poc.migration.data.DatasetGenerator;
import org.example.poc.migration.data.Event;
import org.example.poc.migration.env.EnvironmentSetup;
import org.example.poc.migration.transform.EventTransformer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT-1 (test-plan §3.1): after dataset generation, {@code events_legacy} contains exactly
 * N entries. Also exercises the Phase-5 surface: {@link EnvironmentSetup} can start
 * MiniAccumulo, create the six PoC tables, and apply per-table split points produced by
 * {@link DatasetGenerator}.
 *
 * <p>Uses Failsafe ({@code *IT.java}), so a plain {@code mvn test} does NOT run it —
 * cluster bootstrap is in the 30–60s range. Run with {@code mvn verify}.
 *
 * <p>The cluster is shared across the test methods in this class
 * ({@link TestInstance.Lifecycle#PER_CLASS}); this is the most expensive resource in the
 * test plan and there is no per-method state to isolate.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DatasetGenerationIT {

    /** Small enough to keep the cluster cycle quick; large enough to land splits. */
    private static final int TOTAL_EVENTS = 100;
    private static final int UNIQUE_USERS = 10;
    private static final int UNIQUE_SESSIONS = 25;
    private static final long TIME_START = 1_735_689_600_000L; // 2025-01-01
    private static final long TIME_END   = 1_738_368_000_000L; // 2025-02-01
    private static final long SEED = 42L;

    @TempDir
    static Path tempDir;

    private EnvironmentSetup env;
    private List<Event> generatedEvents;

    @BeforeAll
    void setUp() throws Exception {
        env = new EnvironmentSetup(tempDir.resolve("mini-accumulo"), "secret");
        env.start();
        env.createTables();

        DatasetGenerator generator = new DatasetGenerator(
                TOTAL_EVENTS, UNIQUE_USERS, UNIQUE_SESSIONS, TIME_START, TIME_END, SEED);
        generatedEvents = generator.generate();
        Map<String, SortedSet<Text>> splits = generator.splitPoints(generatedEvents);
        for (Map.Entry<String, SortedSet<Text>> e : splits.entrySet()) {
            env.applySplits(e.getKey(), e.getValue());
        }
        generator.writeToLegacy(env.client(), EnvironmentSetup.SOURCE_TABLE, generatedEvents);
    }

    @AfterAll
    void tearDown() throws Exception {
        if (env != null) {
            env.close();
        }
    }

    @Test
    void it1_eventsLegacyContainsExactlyN() throws Exception {
        AccumuloClient client = env.client();
        long count;
        try (Scanner scanner = client.createScanner(
                EnvironmentSetup.SOURCE_TABLE, Authorizations.EMPTY)) {
            scanner.setRange(new Range());
            count = scanner.stream().count();
        }
        assertEquals(TOTAL_EVENTS, count,
                "events_legacy must contain exactly " + TOTAL_EVENTS + " entries");
    }

    @Test
    void generate_isDeterministic_acrossInstances() {
        DatasetGenerator other = new DatasetGenerator(
                TOTAL_EVENTS, UNIQUE_USERS, UNIQUE_SESSIONS, TIME_START, TIME_END, SEED);
        List<Event> twice = other.generate();
        assertEquals(generatedEvents, twice,
                "same config + seed must produce equal event lists (NFR-3)");
    }

    @Test
    void splitPoints_areAppliedToTargetTables() throws Exception {
        AccumuloClient client = env.client();
        for (String table : List.of(
                EventTransformer.EVENTS_BY_ID,
                EventTransformer.EVENTS_BY_USER,
                EventTransformer.EVENTS_BY_SESSION,
                EventTransformer.EVENT_COMPONENTS_SEARCHABLE,
                EventTransformer.EVENT_STATS_BY_TYPE)) {
            var listed = client.tableOperations().listSplits(table);
            assertNotNull(listed, "listSplits returned null for " + table);
            assertFalse(listed.isEmpty(),
                    "expected at least one split on " + table + " — found none");
        }
    }

    @Test
    void allSixTablesExist() throws Exception {
        var tables = env.client().tableOperations().list();
        for (String t : EnvironmentSetup.ALL_TABLES) {
            assertTrue(tables.contains(t), "missing table: " + t);
        }
    }
}
