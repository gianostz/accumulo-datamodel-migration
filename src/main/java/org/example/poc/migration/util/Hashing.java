package org.example.poc.migration.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;

/**
 * SHA-256 helpers for CC-4 aggregate checksum (test-plan §4).
 *
 * {@link #hashSortedList(Iterable)} is the canonical form: sort lexicographically, join
 * with {@code '\n'} (no trailing newline), hash. Both sides of CC-4 must use this exact
 * shape or the checksum will not match.
 */
public final class Hashing {

    private static final String ALGORITHM = "SHA-256";
    private static final byte[] SEPARATOR = "\n".getBytes(StandardCharsets.UTF_8);

    private Hashing() {}

    public static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance(ALGORITHM).digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(ALGORITHM + " not available in this JVM", e);
        }
    }

    public static String sha256Hex(byte[] input) {
        return HexFormat.of().formatHex(sha256(input));
    }

    /**
     * Sorts the input lexicographically, then hashes the {@code '\n'}-joined UTF-8 bytes.
     * Returns lowercase hex. CC-4 compares two such hashes — both producers must agree on
     * sort order and separator, hence this single canonical implementation.
     */
    public static String hashSortedList(Iterable<String> items) {
        List<String> sorted = new ArrayList<>();
        for (String item : items) {
            sorted.add(item);
        }
        Collections.sort(sorted);

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(ALGORITHM + " not available in this JVM", e);
        }
        boolean first = true;
        for (String item : sorted) {
            if (first) {
                first = false;
            } else {
                digest.update(SEPARATOR);
            }
            digest.update(item.getBytes(StandardCharsets.UTF_8));
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
