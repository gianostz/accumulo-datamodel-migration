package org.example.poc.migration.data;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NFR-3 round-trip guarantees:
 *  1. {@code deserialize(serialize(e)).equals(e)} — object identity preserved.
 *  2. {@code serialize(serialize.deserialize(canonicalJson))} is byte-identical to the
 *     input canonical JSON — proves alphabetical-property ordering pins output bytes,
 *     which is what makes the migrated value re-importable without diff.
 *
 *  The canonical JSON literal below must have its keys in alphabetical order, because
 *  that is the order the serializer emits.
 */
class EventSerializerTest {

    private final EventSerializer serializer = new EventSerializer();

    private static final Event SAMPLE = new Event(
            "evt-7f3a9b21",
            "user-042",
            "sess-a1b2c3",
            "LOGIN",
            1_736_510_400_000L,
            "192.168.1.42",
            "Mozilla/5.0 ...",
            "/api/orders/123");

    @Test
    void objectRoundTrip_preservesAllFields() {
        byte[] json = serializer.serialize(SAMPLE);
        Event back = serializer.deserialize(json);
        assertEquals(SAMPLE, back);
    }

    @Test
    void serialize_isAlphabeticallyOrdered() {
        String json = serializer.serializeToString(SAMPLE);
        // First key must be eventId (alphabetically first of the 8 fields).
        assertTrue(json.startsWith("{\"eventId\":"),
                "expected eventId first, was: " + json);
        // Property order check: each subsequent key follows the previous alphabetically.
        String[] keysInOrder = {
                "eventId", "eventType", "ipAddress", "resourceAccessed",
                "sessionId", "timestamp", "userAgent", "userId"
        };
        int last = -1;
        for (String key : keysInOrder) {
            int idx = json.indexOf("\"" + key + "\":");
            assertTrue(idx > last,
                    "key " + key + " out of order in: " + json);
            last = idx;
        }
    }

    @Test
    void byteRoundTrip_fromCanonicalJson_isStable() {
        // Canonical: keys in alphabetical order — same order the serializer emits.
        String canonical = "{"
                + "\"eventId\":\"evt-001\","
                + "\"eventType\":\"LOGIN\","
                + "\"ipAddress\":\"10.0.0.5\","
                + "\"resourceAccessed\":\"/auth/login\","
                + "\"sessionId\":\"sess-xyz\","
                + "\"timestamp\":1736510400000,"
                + "\"userAgent\":\"curl/7.85\","
                + "\"userId\":\"user-042\""
                + "}";
        Event parsed = serializer.deserialize(canonical);
        byte[] reSerialized = serializer.serialize(parsed);

        assertArrayEquals(canonical.getBytes(StandardCharsets.UTF_8), reSerialized,
                "round-trip changed bytes; got: " + new String(reSerialized, StandardCharsets.UTF_8));
    }

    @Test
    void serialize_twoInvocations_areByteIdentical() {
        // NFR-3 idempotency basis: serializing the same Event twice in the same JVM must
        // produce identical bytes (no map-iteration leakage, no hashcode-driven ordering).
        byte[] a = serializer.serialize(SAMPLE);
        byte[] b = serializer.serialize(SAMPLE);
        assertTrue(Arrays.equals(a, b));
    }
}
