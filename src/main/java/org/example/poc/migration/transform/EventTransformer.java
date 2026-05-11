package org.example.poc.migration.transform;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.example.poc.migration.data.Event;
import org.example.poc.migration.data.EventSerializer;
import org.example.poc.migration.util.KeyUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure 1→7 transformation from one source {@code events_legacy} KeyValue to seven target
 * entries across the five new tables (data-model §5).
 *
 * <p>Two invariants this class is responsible for:
 * <ul>
 *   <li><b>Determinism / idempotency (NFR-3).</b> Output is a pure function of the inputs.
 *       Every produced {@link Key} carries the <i>source</i> KeyValue timestamp
 *       (architecture §5.4) — never {@code now()}, never the JSON {@code event.timestamp}.
 *       This is what makes re-import a no-op.</li>
 *   <li><b>Client bypass (NFR-4).</b> This class lives in {@code transform/} and may only
 *       depend on Accumulo {@code data.*} types ({@link Key}, {@link Value}) plus the
 *       project's own pure helpers. The UT-6 ArchUnit rule fails the build if anything
 *       under {@code org.apache.accumulo.core.client..} leaks in here.</li>
 * </ul>
 */
public final class EventTransformer {

    public static final String EVENTS_BY_ID = "events_by_id";
    public static final String EVENTS_BY_USER = "events_by_user";
    public static final String EVENTS_BY_SESSION = "events_by_session";
    public static final String EVENT_COMPONENTS_SEARCHABLE = "event_components_searchable";
    public static final String EVENT_STATS_BY_TYPE = "event_stats_by_type";

    private static final String CF_EVENT = "event";
    private static final String CQ_DATA = "data";
    private static final String CF_REF = "ref";
    private static final String CQ_EVENT_ID = "eventId";
    private static final String CF_IDX = "idx";
    private static final String CQ_IP = "ip";
    private static final String CQ_RESOURCE = "resource";
    private static final String CQ_TYPE = "type";
    private static final String CF_STAT = "stat";
    private static final String CQ_COUNT = "count";
    private static final String VIS_EMPTY = "";

    private static final String PREFIX_IP = "ip_";
    private static final String PREFIX_RESOURCE = "resource_";
    private static final String PREFIX_TYPE = "type_";
    private static final String SEP = "_";

    private final EventSerializer serializer;

    public EventTransformer() {
        this(new EventSerializer());
    }

    public EventTransformer(EventSerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Fan a source event out into the seven target entries (data-model §5).
     * Order of the returned list is stable across invocations for the same input.
     */
    public List<TargetEntry> transform(Key sourceKey, Value sourceValue) {
        long sourceTs = sourceKey.getTimestamp();
        Event event = serializer.deserialize(sourceValue.get());

        List<TargetEntry> out = new ArrayList<>(7);

        // 1. events_by_id — full event JSON, keyed by eventId.
        out.add(new TargetEntry(
                EVENTS_BY_ID,
                new Key(event.eventId(), CF_EVENT, CQ_DATA, VIS_EMPTY, sourceTs),
                new Value(serializer.serialize(event))));

        // 2. events_by_user — userId_<reverseTs>_<eventId>, reverse-chronological per user.
        out.add(new TargetEntry(
                EVENTS_BY_USER,
                new Key(
                        event.userId() + SEP + KeyUtils.reverseTs(event.timestamp()) + SEP + event.eventId(),
                        CF_REF, CQ_EVENT_ID, VIS_EMPTY, sourceTs),
                new Value(event.eventId())));

        // 3. events_by_session — sessionId_<pad(ts)>_<eventId>, ascending within a session.
        out.add(new TargetEntry(
                EVENTS_BY_SESSION,
                new Key(
                        event.sessionId() + SEP + KeyUtils.pad(event.timestamp()) + SEP + event.eventId(),
                        CF_REF, CQ_EVENT_ID, VIS_EMPTY, sourceTs),
                new Value(event.eventId())));

        // 4. event_components_searchable — three secondary-index entries (ip, resource, type).
        out.add(new TargetEntry(
                EVENT_COMPONENTS_SEARCHABLE,
                new Key(
                        PREFIX_IP + event.ipAddress() + SEP + event.eventId(),
                        CF_IDX, CQ_IP, VIS_EMPTY, sourceTs),
                new Value(event.eventId())));
        out.add(new TargetEntry(
                EVENT_COMPONENTS_SEARCHABLE,
                new Key(
                        PREFIX_RESOURCE + event.resourceAccessed() + SEP + event.eventId(),
                        CF_IDX, CQ_RESOURCE, VIS_EMPTY, sourceTs),
                new Value(event.eventId())));
        out.add(new TargetEntry(
                EVENT_COMPONENTS_SEARCHABLE,
                new Key(
                        PREFIX_TYPE + event.eventType() + SEP + event.eventId(),
                        CF_IDX, CQ_TYPE, VIS_EMPTY, sourceTs),
                new Value(event.eventId())));

        // 5. event_stats_by_type — eventType_<yyyyMMdd>_<eventId>, distinct entry per eventId
        //    (data-model §4.5 note: count is obtained by scan-counting, not a combiner).
        out.add(new TargetEntry(
                EVENT_STATS_BY_TYPE,
                new Key(
                        event.eventType() + SEP + KeyUtils.yyyyMMdd(event.timestamp()) + SEP + event.eventId(),
                        CF_STAT, CQ_COUNT, VIS_EMPTY, sourceTs),
                new Value(event.eventId())));

        return out;
    }
}
