package org.example.poc.migration.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UT-3 — {@code reverseTs(t1) > reverseTs(t2)} lexicographically when {@code t1 < t2}.
 * Plus the surrounding invariants that {@code KeyUtils} promises to its callers
 * (fixed width, UTC date, padded zero).
 */
class KeyUtilsTest {

    @Test
    void pad_zeroProducesNineteenZeroes() {
        assertEquals("0000000000000000000", KeyUtils.pad(0L));
        assertEquals(19, KeyUtils.pad(0L).length());
    }

    @Test
    void pad_maxLongFitsInNineteenDigits() {
        String padded = KeyUtils.pad(Long.MAX_VALUE);
        assertEquals(19, padded.length());
        assertEquals("9223372036854775807", padded);
    }

    @Test
    void pad_arbitraryValuePreservesNumericOrder() {
        assertTrue(KeyUtils.pad(1L).compareTo(KeyUtils.pad(2L)) < 0);
        assertTrue(KeyUtils.pad(999L).compareTo(KeyUtils.pad(1000L)) < 0);
    }

    @Test
    void reverseTs_isLexicographicallyDescending() {
        long t1 = 1_000_000_000L;
        long t2 = 2_000_000_000L;

        String r1 = KeyUtils.reverseTs(t1);
        String r2 = KeyUtils.reverseTs(t2);

        assertEquals(19, r1.length());
        assertEquals(19, r2.length());
        // t1 < t2 ⇒ reverseTs(t1) sorts AFTER reverseTs(t2)
        assertTrue(r2.compareTo(r1) < 0,
                "reverseTs(" + t2 + ") should sort before reverseTs(" + t1 + ")");
    }

    @Test
    void reverseTs_matchesPadOfComplement() {
        long ts = 1_736_510_400_000L;
        assertEquals(KeyUtils.pad(Long.MAX_VALUE - ts), KeyUtils.reverseTs(ts));
    }

    @Test
    void yyyyMMdd_isUtcAnchored() {
        // 2026-01-10 00:00:00 UTC
        assertEquals("20260110", KeyUtils.yyyyMMdd(1_768_003_200_000L));
        // epoch
        assertEquals("19700101", KeyUtils.yyyyMMdd(0L));
    }

    @Test
    void yyyyMMdd_doesNotDriftAcrossDayBoundaryInOtherZones() {
        // 23:00 UTC on 2026-01-10 is 00:00 next day in CET, but we lock to UTC.
        long elevenPmUtc = 1_768_086_000_000L; // 2026-01-10T23:00:00Z
        assertEquals("20260110", KeyUtils.yyyyMMdd(elevenPmUtc));
    }

    @Test
    void constructor_isPrivate() {
        // Sanity: ensure the class can't be instantiated. Reflection would still work,
        // but the public surface stays static-only.
        assertThrows(IllegalAccessException.class,
                () -> KeyUtils.class.getDeclaredConstructor().newInstance());
    }
}
