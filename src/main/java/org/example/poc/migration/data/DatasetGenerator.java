package org.example.poc.migration.data;

import com.typesafe.config.Config;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.example.poc.migration.transform.EventTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Generates a deterministic synthetic dataset of {@link Event}s and writes it to the
 * {@code events_legacy} source table per data-model §3.
 *
 * <p>Determinism (NFR-3): everything is a pure function of the {@link Config} {@code dataset.*}
 * block. eventIds are issued sequentially as zero-padded hex; userId/sessionId/type/ip/
 * resource/userAgent come from fixed pools, indexed by a {@link Random} seeded from
 * {@code dataset.randomSeed}; the source ingestion timestamp is also deterministic
 * ({@code timeRangeEndMillis + i}) so re-running the generator with the same config
 * produces byte-identical legacy keys.
 *
 * <p>{@link #splitPoints(List)} returns the split points to apply to the five target tables
 * before bulk import (architecture §5.2). The values are computed from the <i>actual</i>
 * generated distribution so a config change that shrinks the dataset still yields balanced
 * tablets.
 */
public final class DatasetGenerator {

    private static final Logger log = LoggerFactory.getLogger(DatasetGenerator.class);

    private static final String SOURCE_CF = "event";
    private static final String SOURCE_CQ = "data";

    public static final String[] EVENT_TYPES = {"LOGIN", "LOGOUT", "RESOURCE_ACCESS", "ACTION"};

    private static final String[] IP_POOL = {
            "10.0.0.5", "10.0.0.12", "10.0.1.7", "192.168.1.42",
            "192.168.1.108", "172.16.4.21", "203.0.113.9", "198.51.100.33"};

    private static final String[] RESOURCE_POOL = {
            "/api/orders/123", "/api/orders/456", "/api/users/42",
            "/auth/login", "/auth/logout", "/api/reports/q1",
            "/dashboard", "/settings/profile"};

    private static final String[] USER_AGENT_POOL = {
            "Mozilla/5.0 (X11; Linux x86_64)",
            "curl/7.85",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15)",
            "PostmanRuntime/7.32"};

    private final int totalEvents;
    private final int uniqueUsers;
    private final int uniqueSessions;
    private final long timeRangeStartMillis;
    private final long timeRangeEndMillis;
    private final long randomSeed;

    private final EventSerializer serializer = new EventSerializer();

    public DatasetGenerator(int totalEvents,
                            int uniqueUsers,
                            int uniqueSessions,
                            long timeRangeStartMillis,
                            long timeRangeEndMillis,
                            long randomSeed) {
        if (totalEvents <= 0) {
            throw new IllegalArgumentException("totalEvents must be > 0, got " + totalEvents);
        }
        if (uniqueUsers <= 0 || uniqueSessions <= 0) {
            throw new IllegalArgumentException(
                    "uniqueUsers and uniqueSessions must be > 0; got "
                            + uniqueUsers + "/" + uniqueSessions);
        }
        if (timeRangeEndMillis <= timeRangeStartMillis) {
            throw new IllegalArgumentException(
                    "timeRangeEndMillis must be > timeRangeStartMillis");
        }
        this.totalEvents = totalEvents;
        this.uniqueUsers = uniqueUsers;
        this.uniqueSessions = uniqueSessions;
        this.timeRangeStartMillis = timeRangeStartMillis;
        this.timeRangeEndMillis = timeRangeEndMillis;
        this.randomSeed = randomSeed;
    }

    public static DatasetGenerator fromConfig(Config config) {
        return new DatasetGenerator(
                config.getInt("dataset.totalEvents"),
                config.getInt("dataset.uniqueUsers"),
                config.getInt("dataset.uniqueSessions"),
                config.getLong("dataset.timeRangeStartMillis"),
                config.getLong("dataset.timeRangeEndMillis"),
                config.getLong("dataset.randomSeed"));
    }

    public int totalEvents() { return totalEvents; }
    public int uniqueUsers() { return uniqueUsers; }
    public int uniqueSessions() { return uniqueSessions; }

    /**
     * Build the in-memory list of events. Order is the issue order (matches the order they
     * will be written to {@code events_legacy}). Re-invoking with the same config returns
     * an equal list (NFR-3).
     */
    public List<Event> generate() {
        Random random = new Random(randomSeed);
        int userPadWidth = digits(uniqueUsers - 1);
        int sessionPadWidth = digits(uniqueSessions - 1);
        long timeSpan = timeRangeEndMillis - timeRangeStartMillis;

        List<Event> events = new ArrayList<>(totalEvents);
        for (int i = 0; i < totalEvents; i++) {
            String eventId = String.format("evt-%08x", i);
            String userId = "user-" + pad(random.nextInt(uniqueUsers), userPadWidth);
            String sessionId = "sess-" + pad(random.nextInt(uniqueSessions), sessionPadWidth);
            String eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
            long ts = timeRangeStartMillis + (long) (random.nextDouble() * timeSpan);
            String ip = IP_POOL[random.nextInt(IP_POOL.length)];
            String userAgent = USER_AGENT_POOL[random.nextInt(USER_AGENT_POOL.length)];
            String resource = RESOURCE_POOL[random.nextInt(RESOURCE_POOL.length)];
            events.add(new Event(eventId, userId, sessionId, eventType, ts, ip, userAgent, resource));
        }
        return events;
    }

    /**
     * Write the given events to {@code events_legacy} in the legacy schema
     * (data-model §3): rowId = {@code userId_timestamp}, CF=event, CQ=data, value = JSON.
     * The KeyValue timestamp is a deterministic function of position
     * ({@code timeRangeEndMillis + i}), so re-running produces byte-identical entries —
     * Accumulo's versioning will overwrite rather than duplicate.
     *
     * <p>Returns the number of mutations submitted. A subsequent count scan on the source
     * table is the authoritative confirmation (used by IT-1).
     */
    public long writeToLegacy(AccumuloClient client, String table, List<Event> events) throws Exception {
        long count = 0;
        try (BatchWriter writer = client.createBatchWriter(table, new BatchWriterConfig())) {
            for (int i = 0; i < events.size(); i++) {
                Event e = events.get(i);
                Mutation mutation = new Mutation(e.userId() + "_" + e.timestamp());
                long ingestTs = timeRangeEndMillis + i;
                mutation.put(SOURCE_CF, SOURCE_CQ, ingestTs, serializer.serializeToString(e));
                writer.addMutation(mutation);
                count++;
            }
        }
        log.info("Wrote {} events to {}", count, table);
        return count;
    }

    /**
     * Compute split points for the 5 target tables from the generated dataset
     * (architecture §5.2). Each table gets 3 splits (4 tablets) except
     * {@code event_components_searchable} (2 splits, one tablet per indexed field) — the
     * shapes match the examples in data-model §8.
     */
    public Map<String, SortedSet<Text>> splitPoints(List<Event> events) {
        Map<String, SortedSet<Text>> all = new LinkedHashMap<>();

        all.put(EventTransformer.EVENTS_BY_ID,
                quartileSplits(events.stream().map(Event::eventId).sorted().toList()));

        all.put(EventTransformer.EVENTS_BY_USER,
                quartileSplits(events.stream().map(Event::userId).distinct().sorted().toList()));

        all.put(EventTransformer.EVENTS_BY_SESSION,
                quartileSplits(events.stream().map(Event::sessionId).distinct().sorted().toList()));

        // event_components_searchable rows look like "<field>_<value>_<eventId>". A split at
        // "<field>_~" (~ = 0x7E, above any printable char we use in IPs/types/resources)
        // places all rows whose row begins with "<field>_" — but not the next field's rows —
        // in the same tablet. Two splits → three tablets, one per indexed field.
        SortedSet<Text> componentSplits = new TreeSet<>();
        componentSplits.add(new Text("ip_~"));
        componentSplits.add(new Text("resource_~"));
        all.put(EventTransformer.EVENT_COMPONENTS_SEARCHABLE, componentSplits);

        // event_stats_by_type rows look like "<type>_<yyyyMMdd>_<eventId>". Sorted types are
        // ACTION, LOGIN, LOGOUT, RESOURCE_ACCESS. Splits at "LOGIN", "LOGOUT",
        // "RESOURCE_ACCESS" — values strictly less than the corresponding "<type>_..." rows
        // — isolate each type in its own tablet (data-model §8 example).
        SortedSet<Text> statSplits = new TreeSet<>();
        statSplits.add(new Text("LOGIN"));
        statSplits.add(new Text("LOGOUT"));
        statSplits.add(new Text("RESOURCE_ACCESS"));
        all.put(EventTransformer.EVENT_STATS_BY_TYPE, statSplits);

        return all;
    }

    /**
     * Pick the 25%/50%/75% positions from a sorted list as split points. Returned values
     * are strictly less than the largest element, so the tail tablet is never empty. If
     * the list is too small to yield three distinct splits, fewer are returned.
     */
    private static SortedSet<Text> quartileSplits(List<String> sortedValues) {
        SortedSet<Text> splits = new TreeSet<>();
        if (sortedValues.size() < 4) {
            return splits;
        }
        int n = sortedValues.size();
        int[] positions = {n / 4, n / 2, (3 * n) / 4};
        String last = sortedValues.get(n - 1);
        for (int p : positions) {
            String candidate = sortedValues.get(Math.min(p, n - 1));
            if (candidate.compareTo(last) < 0) {
                splits.add(new Text(candidate));
            }
        }
        return Collections.unmodifiableSortedSet(splits);
    }

    private static int digits(int n) {
        return Math.max(1, Integer.toString(Math.abs(n)).length());
    }

    private static String pad(int n, int width) {
        return String.format("%0" + width + "d", n);
    }
}
