package org.example.poc.migration.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class HashingTest {

    @Test
    void sha256Hex_matchesKnownVector() {
        // Empty input: SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assertEquals(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                Hashing.sha256Hex(new byte[0]));
    }

    @Test
    void sha256Hex_abc() {
        // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        assertEquals(
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                Hashing.sha256Hex("abc".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void hashSortedList_isOrderInsensitive() {
        String h1 = Hashing.hashSortedList(List.of("evt-a", "evt-b", "evt-c"));
        String h2 = Hashing.hashSortedList(List.of("evt-c", "evt-a", "evt-b"));
        assertEquals(h1, h2);
    }

    @Test
    void hashSortedList_matchesNewlineJoinedSha() {
        // CC-4 contract: join the sorted list with '\n' and hash the UTF-8 bytes.
        String h = Hashing.hashSortedList(List.of("b", "a", "c"));
        String expected = Hashing.sha256Hex("a\nb\nc".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, h);
    }

    @Test
    void hashSortedList_emptyHasFixedValue() {
        String h = Hashing.hashSortedList(List.of());
        assertEquals(Hashing.sha256Hex(new byte[0]), h);
    }

    @Test
    void hashSortedList_differentContentDiffers() {
        assertNotEquals(
                Hashing.hashSortedList(List.of("a", "b")),
                Hashing.hashSortedList(List.of("a", "c")));
    }

    @Test
    void sha256_byteArrayLength() {
        assertEquals(32, Hashing.sha256(new byte[0]).length);
        assertEquals(32, Hashing.sha256("anything".getBytes(StandardCharsets.UTF_8)).length);
    }

    @Test
    void sha256_returnsDistinctArray() {
        byte[] a = Hashing.sha256("x".getBytes(StandardCharsets.UTF_8));
        byte[] b = Hashing.sha256("x".getBytes(StandardCharsets.UTF_8));
        // Same bytes...
        assertEquals(true, Arrays.equals(a, b));
        // ...but not the same reference (digest returns fresh array each time).
        assertNotEquals(System.identityHashCode(a), System.identityHashCode(b));
    }
}
