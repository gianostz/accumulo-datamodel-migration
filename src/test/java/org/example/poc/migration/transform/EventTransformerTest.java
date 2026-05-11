package org.example.poc.migration.transform;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.example.poc.migration.data.Event;
import org.example.poc.migration.data.EventSerializer;
import org.example.poc.migration.util.KeyUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UT-1 (fan-out shape) and UT-2 (idempotency) for {@link EventTransformer}, plus the
 * rowId/CF/CQ contract from data-model §5 and the source-timestamp invariant from
 * architecture §5.4.
 *
 * The sample uses a deliberately distinct source ingestion timestamp ({@link #SOURCE_TS})
 * versus {@code event.timestamp} so the test can prove the produced Key carries the
 * <i>source</i> timestamp — not the payload one.
 */
class EventTransformerTest {

    private static final EventSerializer SERIALIZER = new EventSerializer();
    private static final EventTransformer TRANSFORMER = new EventTransformer();

    private static final Event SAMPLE = new Event(
            "evt-001",
            "user-042",
            "sess-xyz",
            "LOGIN",
            1_736_510_400_000L,
            "10.0.0.5",
            "curl/7.85",
            "/auth/login");

    /** Source KeyValue ingestion timestamp. Deliberately distinct from {@code SAMPLE.timestamp()}. */
    private static final long SOURCE_TS = 9_999_999_999L;

    private static Key sourceKey() {
        // legacy rowId per data-model §3: userId_timestamp
        return new Key(
                SAMPLE.userId() + "_" + SAMPLE.timestamp(),
                "event", "data", "", SOURCE_TS);
    }

    private static Value sourceValue() {
        return new Value(SERIALIZER.serialize(SAMPLE));
    }

    @Test
    void ut1_fanout_isSeven_distributed_1_1_1_3_1() {
        List<TargetEntry> out = TRANSFORMER.transform(sourceKey(), sourceValue());

        assertEquals(7, out.size(), "expected 7 entries (1+1+1+3+1)");

        Map<String, Long> byTable = out.stream()
                .collect(Collectors.groupingBy(TargetEntry::targetTable, Collectors.counting()));
        assertEquals(1L, byTable.get(EventTransformer.EVENTS_BY_ID));
        assertEquals(1L, byTable.get(EventTransformer.EVENTS_BY_USER));
        assertEquals(1L, byTable.get(EventTransformer.EVENTS_BY_SESSION));
        assertEquals(3L, byTable.get(EventTransformer.EVENT_COMPONENTS_SEARCHABLE));
        assertEquals(1L, byTable.get(EventTransformer.EVENT_STATS_BY_TYPE));
    }

    @Test
    void ut2_idempotent_byteForByte_acrossInvocations() {
        List<TargetEntry> a = TRANSFORMER.transform(sourceKey(), sourceValue());
        List<TargetEntry> b = TRANSFORMER.transform(sourceKey(), sourceValue());

        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            TargetEntry ai = a.get(i);
            TargetEntry bi = b.get(i);
            assertEquals(ai.targetTable(), bi.targetTable(), "table @ " + i);
            // Key.equals compares row, cf, cq, cv, ts, deleted — sufficient for byte-identity.
            assertEquals(ai.key(), bi.key(), "key @ " + i);
            assertEquals(ai.key().getTimestamp(), bi.key().getTimestamp(), "ts @ " + i);
            assertArrayEquals(ai.value().get(), bi.value().get(), "value @ " + i);
        }
    }

    @Test
    void everyProducedKey_carriesSourceTimestamp() {
        // Architecture §5.4: produced keys must reuse the source KeyValue timestamp, not
        // event.timestamp (the JSON payload). Otherwise re-import is not idempotent.
        List<TargetEntry> out = TRANSFORMER.transform(sourceKey(), sourceValue());
        for (TargetEntry e : out) {
            assertEquals(SOURCE_TS, e.key().getTimestamp(),
                    "expected source ts on " + e.targetTable() + " row=" + e.key().getRow());
            assertTrue(SOURCE_TS != SAMPLE.timestamp(),
                    "test fixture broken: SOURCE_TS must differ from event.timestamp");
        }
    }

    @Test
    void rowIds_match_dataModel_section5() {
        Map<String, List<TargetEntry>> byTable = TRANSFORMER.transform(sourceKey(), sourceValue())
                .stream()
                .collect(Collectors.groupingBy(TargetEntry::targetTable));

        // 1. events_by_id — row=eventId, cf=event, cq=data, value=canonical event JSON.
        TargetEntry byId = byTable.get(EventTransformer.EVENTS_BY_ID).get(0);
        assertEquals("evt-001", byId.key().getRow().toString());
        assertEquals("event", byId.key().getColumnFamily().toString());
        assertEquals("data", byId.key().getColumnQualifier().toString());
        assertArrayEquals(SERIALIZER.serialize(SAMPLE), byId.value().get());

        // 2. events_by_user — userId_<reverseTs>_<eventId>.
        TargetEntry byUser = byTable.get(EventTransformer.EVENTS_BY_USER).get(0);
        String expectedUserRow = "user-042_" + KeyUtils.reverseTs(SAMPLE.timestamp()) + "_evt-001";
        assertEquals(expectedUserRow, byUser.key().getRow().toString());
        assertEquals("ref", byUser.key().getColumnFamily().toString());
        assertEquals("eventId", byUser.key().getColumnQualifier().toString());
        assertEquals("evt-001", new String(byUser.value().get(), StandardCharsets.UTF_8));

        // 3. events_by_session — sessionId_<pad(ts)>_<eventId>.
        TargetEntry bySession = byTable.get(EventTransformer.EVENTS_BY_SESSION).get(0);
        String expectedSessionRow = "sess-xyz_" + KeyUtils.pad(SAMPLE.timestamp()) + "_evt-001";
        assertEquals(expectedSessionRow, bySession.key().getRow().toString());
        assertEquals("ref", bySession.key().getColumnFamily().toString());
        assertEquals("eventId", bySession.key().getColumnQualifier().toString());
        assertEquals("evt-001", new String(bySession.value().get(), StandardCharsets.UTF_8));

        // 4. event_components_searchable — exactly the three CQs ip / resource / type.
        List<TargetEntry> ec = byTable.get(EventTransformer.EVENT_COMPONENTS_SEARCHABLE);
        Map<String, TargetEntry> byCq = ec.stream()
                .collect(Collectors.toMap(e -> e.key().getColumnQualifier().toString(), e -> e));
        assertEquals(3, byCq.size(), "expected ip/resource/type — got " + byCq.keySet());
        assertEquals("ip_10.0.0.5_evt-001", byCq.get("ip").key().getRow().toString());
        assertEquals("resource_/auth/login_evt-001", byCq.get("resource").key().getRow().toString());
        assertEquals("type_LOGIN_evt-001", byCq.get("type").key().getRow().toString());
        for (TargetEntry e : ec) {
            assertEquals("idx", e.key().getColumnFamily().toString());
            assertEquals("evt-001", new String(e.value().get(), StandardCharsets.UTF_8));
        }

        // 5. event_stats_by_type — eventType_<yyyyMMdd>_<eventId>.
        TargetEntry stats = byTable.get(EventTransformer.EVENT_STATS_BY_TYPE).get(0);
        String expectedStatRow = "LOGIN_" + KeyUtils.yyyyMMdd(SAMPLE.timestamp()) + "_evt-001";
        assertEquals(expectedStatRow, stats.key().getRow().toString());
        assertEquals("stat", stats.key().getColumnFamily().toString());
        assertEquals("count", stats.key().getColumnQualifier().toString());
        assertEquals("evt-001", new String(stats.value().get(), StandardCharsets.UTF_8));
    }

    @Test
    void eventsById_value_isCanonicalReSerialization_notSourceBytesPassThrough() {
        // Defensive: even if the caller hands us a non-canonical JSON Value, we must emit
        // the canonical one — that is what makes NFR-3 byte-identity hold for events_by_id
        // across runs, independent of how the source was originally serialized.
        String nonCanonical =
                "{\"userId\":\"user-042\",\"eventId\":\"evt-001\",\"sessionId\":\"sess-xyz\","
                        + "\"eventType\":\"LOGIN\",\"timestamp\":1736510400000,"
                        + "\"ipAddress\":\"10.0.0.5\",\"userAgent\":\"curl/7.85\","
                        + "\"resourceAccessed\":\"/auth/login\"}";
        Key src = new Key("user-042_1736510400000", "event", "data", "", SOURCE_TS);
        Value srcVal = new Value(nonCanonical.getBytes(StandardCharsets.UTF_8));

        List<TargetEntry> out = TRANSFORMER.transform(src, srcVal);
        TargetEntry byId = out.stream()
                .filter(e -> EventTransformer.EVENTS_BY_ID.equals(e.targetTable()))
                .findFirst().orElseThrow();
        assertArrayEquals(SERIALIZER.serialize(SAMPLE), byId.value().get());
    }
}
