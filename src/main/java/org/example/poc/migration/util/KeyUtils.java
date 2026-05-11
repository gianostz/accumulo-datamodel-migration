package org.example.poc.migration.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Pure helpers for target rowId construction (data-model §5).
 *
 * Determinism (NFR-3): no {@code now()}, no system locale, no system timezone. Every
 * function is a pure transform from its inputs. The {@code yyyyMMdd} formatter is locked
 * to UTC so output is identical across machines.
 */
public final class KeyUtils {

    public static final int PAD_WIDTH = 19;

    private static final DateTimeFormatter YYYY_MM_DD =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private KeyUtils() {}

    /**
     * Zero-pads {@code n} to {@value #PAD_WIDTH} digits — the width of {@code Long.MAX_VALUE}
     * — so longs become lexicographically comparable strings.
     */
    public static String pad(long n) {
        return String.format("%0" + PAD_WIDTH + "d", n);
    }

    /**
     * {@code pad(Long.MAX_VALUE - ts)}. Larger {@code ts} ⇒ smaller string ⇒ sorts first,
     * giving the reverse-chronological ordering used by {@code events_by_user} (data-model §4.2).
     */
    public static String reverseTs(long ts) {
        return pad(Long.MAX_VALUE - ts);
    }

    /**
     * UTC-locked {@code yyyyMMdd}. Used to bucket events by day for {@code event_stats_by_type}
     * (data-model §4.5). System timezone is deliberately ignored.
     */
    public static String yyyyMMdd(long epochMillis) {
        return YYYY_MM_DD.format(Instant.ofEpochMilli(epochMillis));
    }
}
