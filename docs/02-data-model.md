# PoC Accumulo RFile Migration — Data Model

## 1. Application Domain

The reference domain for the PoC is **tracking access events for a system** (login, logout, resource access, user actions). It is a domain simple enough to understand, yet realistically characterized by all the access patterns that justify fan-out across multiple tables:

- Lookup by ID (existence check, single event detail)
- Lookup by user (history of a user's activity)
- Lookup by session (events correlated to the same working session)
- Attribute-based search (events from a specific IP, events on a specific resource)
- Aggregations (event count by type per day)

## 2. Event Schema (Logical Model)

The event is the central entity. Schema:

```json
{
  "eventId": "evt-7f3a9b21",
  "userId": "user-042",
  "sessionId": "sess-a1b2c3",
  "eventType": "LOGIN | LOGOUT | RESOURCE_ACCESS | ACTION",
  "timestamp": 1736510400000,
  "ipAddress": "192.168.1.42",
  "userAgent": "Mozilla/5.0 ...",
  "resourceAccessed": "/api/orders/123"
}
```

All fields are strings except `timestamp` (long, epoch millis). For the PoC, serialization is **JSON UTF-8**: chosen for ease of inspection and debugging. In production a binary format would be used (Avro, Protobuf, Thrift).

## 3. Legacy Model (Source Table)

### Table: `events_legacy`

| Key Component     | Value                                                    |
|-------------------|----------------------------------------------------------|
| Row ID            | `<userId>_<timestamp>`                                   |
| Column Family     | `event`                                                  |
| Column Qualifier  | `data`                                                   |
| Visibility        | (empty in the PoC)                                       |
| Timestamp         | `System.currentTimeMillis()` at the time of ingestion    |
| Value             | JSON-serialized full event (UTF-8)                       |

**Legacy model rationale**: this is a common form of "raw event store": the primary key is optimized for chronological extraction by user, and the entire payload is in the value. It works well for ingestion and dumps, but is awkward for any access pattern that is not `userId + time`.

### Key/Value Example

```
RowID: user-042_1736510400000
CF:    event
CQ:    data
Value: {"eventId":"evt-7f3a9b21","userId":"user-042","sessionId":"sess-a1b2c3",
        "eventType":"LOGIN","timestamp":1736510400000,"ipAddress":"192.168.1.42",
        "userAgent":"Mozilla/5.0","resourceAccessed":"/login"}
```

## 4. New Model (5 Target Tables)

### 4.1 Table: `events_by_id`

Primary lookup by event identifier. Source of truth for the new model.

| Component | Value                                              |
|-----------|----------------------------------------------------|
| Row ID    | `<eventId>`                                        |
| CF        | `event`                                            |
| CQ        | `data`                                             |
| Value     | JSON-serialized full event (UTF-8)                 |

**Cardinality**: 1 entry per source event.

**Usage pattern**: `GET event by eventId`.

### 4.2 Table: `events_by_user`

A user's events in reverse chronological order (most recent first).

| Component | Value                                                    |
|-----------|----------------------------------------------------------|
| Row ID    | `<userId>_<reverseTimestamp>_<eventId>`                  |
| CF        | `ref`                                                    |
| CQ        | `eventId`                                                |
| Value     | `<eventId>` (UTF-8)                                      |

Where `reverseTimestamp = (Long.MAX_VALUE - timestamp)`, formatted as a zero-padded 19-character string to ensure correct lexicographic ordering.

The `eventId` is also included in the rowId to guarantee **uniqueness** (two events from the same user with identical timestamps do not collide) and **idempotency** of the transformation.

**Cardinality**: 1 entry per source event.

**Usage pattern**: prefix scan on `<userId>_` to retrieve a user's events from most recent to oldest.

### 4.3 Table: `events_by_session`

Events correlated to the same session, in ascending chronological order (to reconstruct the flow).

| Component | Value                                                         |
|-----------|---------------------------------------------------------------|
| Row ID    | `<sessionId>_<timestamp padded to 19 digits>_<eventId>`       |
| CF        | `ref`                                                         |
| CQ        | `eventId`                                                     |
| Value     | `<eventId>` (UTF-8)                                           |

**Cardinality**: 1 entry per source event.

**Usage pattern**: prefix scan on `<sessionId>_` to reconstruct the temporal sequence of a session.

### 4.4 Table: `event_components_searchable`

Secondary index on the "searchable" fields of the event: `ipAddress`, `resourceAccessed`, `eventType`.

| Component | Value                                                         |
|-----------|---------------------------------------------------------------|
| Row ID    | `<fieldName>_<fieldValue>_<eventId>`                          |
| CF        | `idx`                                                         |
| CQ        | `<fieldName>`                                                 |
| Value     | `<eventId>` (UTF-8)                                           |

`<fieldName>` ∈ { `ip`, `resource`, `type` }.

**Cardinality**: **3 entries per source event** (one per indexed field).

**Usage pattern**: prefix scan on `ip_192.168.1.42_` to find all events from that IP; similarly for `resource_` and `type_`.

### 4.5 Table: `event_stats_by_type`

Aggregate by event type and day, useful for reconstructing time series by category.

| Component | Value                                                         |
|-----------|---------------------------------------------------------------|
| Row ID    | `<eventType>_<yyyyMMdd>_<eventId>`                            |
| CF        | `stat`                                                        |
| CQ        | `count`                                                       |
| Value     | `<eventId>` (UTF-8) — count is obtained by scan-counting      |

**Cardinality**: 1 entry per source event.

**Usage pattern**: prefix scan on `LOGIN_20260301_` to count logins on March 1, 2026; or range scan over time intervals per type.

**Design note**: in production, an Accumulo counter (combiner) would be used for the true aggregate. For the PoC, we keep distinct entries per `eventId` because (a) it preserves transformation idempotency, (b) it simplifies count verifications.

## 5. Transformation Rules (Source → Target)

For each KeyValue read from `events_legacy`:

1. **Deserialize** the `value` as JSON → `Event` object.
2. **Verify** that `event.userId + "_" + event.timestamp` matches the source rowId (sanity check; on mismatch, log warning and continue).
3. **Emit** the following `(targetTable, key, value)` tuples:

| # | Target Table                | Constructed RowId                                             | CF     | CQ        | Value           |
|---|-----------------------------|---------------------------------------------------------------|--------|-----------|-----------------|
| 1 | events_by_id                | `event.eventId`                                               | event  | data      | event JSON      |
| 2 | events_by_user              | `event.userId + "_" + pad(MAX_LONG - event.ts) + "_" + event.eventId` | ref | eventId | event.eventId |
| 3 | events_by_session           | `event.sessionId + "_" + pad(event.ts) + "_" + event.eventId` | ref   | eventId   | event.eventId   |
| 4 | event_components_searchable | `ip_` + `event.ipAddress` + `_` + `event.eventId`            | idx    | ip        | event.eventId   |
| 4 | event_components_searchable | `resource_` + `event.resourceAccessed` + `_` + `event.eventId` | idx  | resource  | event.eventId   |
| 4 | event_components_searchable | `type_` + `event.eventType` + `_` + `event.eventId`          | idx    | type      | event.eventId   |
| 5 | event_stats_by_type         | `event.eventType + "_" + yyyyMMdd(event.ts) + "_" + event.eventId` | stat | count | event.eventId |

**Total entries produced per source event: 7** (1 + 1 + 1 + 3 + 1).

Where `pad(n)` = `String.format("%019d", n)`, ensuring fixed width for lexicographic ordering.

## 6. Accumulo Key Properties of Produced Output

All produced target RFiles must comply with:

- **Timestamp**: use the same timestamp as the source KeyValue (preserves temporal semantics and ordering). Alternatively, a fixed "migration" timestamp may be used to facilitate rollback (see Architecture Document, "Rollback Strategy" section).
- **Visibility**: empty (not managed in the PoC).
- **Deletion marker**: none.

## 7. Transformation Determinism

The transformation is **completely deterministic**: for the same source event, the produced keys are byte-identical. No field depends on `now()`, generated UUIDs, randomly seeded hashes, or any other source of non-determinism.

This guarantees:

- **Idempotency**: re-running the transformation on the same input produces identical output → a repeated bulk import overwrites the same keys without creating duplicates.
- **Checksum verification**: output from successive runs can be compared byte-by-byte.

## 8. Target Table Split Points (for Optimal Partitioning)

For the PoC on MiniAccumulo, simple split points will be configured, pre-calculated to ensure a balanced distribution given the synthetic nature of the dataset:

| Table                         | Split points (example for 10k event dataset)                              |
|-------------------------------|---------------------------------------------------------------------------|
| events_by_id                  | `evt-3`, `evt-7` (alphabetic splits on the `evt-<hex>` prefix)           |
| events_by_user                | `user-100`, `user-200`, `user-300` (assuming string-numeric userIds)     |
| events_by_session             | `sess-3`, `sess-7`                                                        |
| event_components_searchable   | `ip_`, `resource_`, `type_` (splits on fieldName prefixes)               |
| event_stats_by_type           | `LOGIN`, `LOGOUT`, `RESOURCE_ACCESS` (splits by eventType)               |

The final split points are configuration parameters of the PoC and must be calculated based on the actual cardinality of the generated dataset.

## 9. End-to-End Example

**Source event** in `events_legacy`:

```
RowID: user-042_1736510400000
CF: event | CQ: data
Value: {"eventId":"evt-001","userId":"user-042","sessionId":"sess-xyz",
        "eventType":"LOGIN","timestamp":1736510400000,"ipAddress":"10.0.0.5",
        "userAgent":"curl/7.85","resourceAccessed":"/auth/login"}
```

**Produced output** (7 entries):

```
events_by_id:
  evt-001 | event:data | {"eventId":"evt-001",...}

events_by_user:
  user-042_9223372036854775807_evt-001 | ref:eventId | evt-001
  (where 9223372036854775807 = Long.MAX_VALUE - 1736510400000, padded)

events_by_session:
  sess-xyz_0000001736510400000_evt-001 | ref:eventId | evt-001

event_components_searchable:
  ip_10.0.0.5_evt-001          | idx:ip       | evt-001
  resource_/auth/login_evt-001 | idx:resource | evt-001
  type_LOGIN_evt-001           | idx:type     | evt-001

event_stats_by_type:
  LOGIN_20250110_evt-001 | stat:count | evt-001
```
