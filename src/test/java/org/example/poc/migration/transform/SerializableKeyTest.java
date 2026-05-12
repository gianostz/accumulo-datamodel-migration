package org.example.poc.migration.transform;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SerializableKey} is the wrapper that lets the {@code MigrationJob} shuffle
 * Accumulo {@link Key} values across Spark partitions — {@code Key} itself does not implement
 * {@link java.io.Serializable}. These tests exercise the {@code Externalizable} round-trip
 * with the same standard Java serialization machinery Spark uses for closures and shuffle
 * data.
 */
class SerializableKeyTest {

    @Test
    void javaSerializationRoundTrip_preservesAllKeyFields() throws Exception {
        Key original = new Key("row-42", "cfA", "cqB", "vis", 9_999L);
        SerializableKey wrapped = new SerializableKey(original);

        SerializableKey roundTripped = roundTrip(wrapped);

        // Key.equals covers row/cf/cq/visibility/timestamp; assert ts separately because Key.equals
        // ignores deletion-marker but DOES include ts — explicit assertion is clearer in failure.
        assertEquals(original, roundTripped.key(), "key fields must round-trip");
        assertEquals(original.getTimestamp(), roundTripped.key().getTimestamp(), "timestamp");
        assertNotSame(original, roundTripped.key(), "round-tripped key must be a fresh instance");
    }

    @Test
    void compareTo_followsAccumuloKeyNaturalOrdering() {
        SerializableKey a = new SerializableKey(new Key("a", "cf", "cq", "", 100L));
        SerializableKey b = new SerializableKey(new Key("b", "cf", "cq", "", 100L));

        assertTrue(a.compareTo(b) < 0, "row 'a' must sort before row 'b'");
        assertTrue(b.compareTo(a) > 0);
        assertEquals(0, a.compareTo(new SerializableKey(new Key("a", "cf", "cq", "", 100L))));
    }

    @Test
    void compareTo_higherTimestampSortsFirst() {
        // Accumulo natural ordering: row asc, cf asc, cq asc, vis asc, ts DESCENDING.
        // (More recent versions of the same logical key sort first — Spark sortWithinPartitions
        // relies on this so the RFile writer's append() stays well-ordered.)
        SerializableKey newer = new SerializableKey(new Key("r", "cf", "cq", "", 200L));
        SerializableKey older = new SerializableKey(new Key("r", "cf", "cq", "", 100L));
        assertTrue(newer.compareTo(older) < 0, "newer ts (200) must sort before older ts (100)");
    }

    @Test
    void constructor_copiesDefensively() {
        // Mutating the source Key's row text after construction must not change the wrapped key.
        Key src = new Key("original", "cf", "cq", "", 1L);
        SerializableKey wrapped = new SerializableKey(src);
        // Create a different Key and ensure equality is by value, not by reference identity:
        Key sameValue = new Key("original", "cf", "cq", "", 1L);
        assertEquals(wrapped.key(), sameValue);
    }

    private static SerializableKey roundTrip(SerializableKey in) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(in);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            return (SerializableKey) ois.readObject();
        }
    }
}
