package org.example.poc.migration.data;

/**
 * Source event schema (data-model §2). Eight fields, immutable, no defaults.
 *
 * Determinism: this record holds no derived state. JSON serialization must be byte-stable
 * across runs — see {@link EventSerializer} for the ordering guarantee that supports NFR-3.
 */
public record Event(
        String eventId,
        String userId,
        String sessionId,
        String eventType,
        long timestamp,
        String ipAddress,
        String userAgent,
        String resourceAccessed) {
}
