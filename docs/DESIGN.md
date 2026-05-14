# Accumulo RFile Migration — PoC Design

## 1. Goal

Demonstrate, end to end, that Accumulo data can be migrated from one data model
to another **without routing the transformation through a TabletServer**:

1. **Read** RFiles directly from disk (`RFile.newScanner()`).
2. **Transform** each entry, fanning it out across several target schemas.
3. **Write** new RFiles directly (`RFile.newWriter()`).
4. **Import** the RFiles into new tables (`TableOperations.importDirectory()`).
5. **Scan** the target tables to confirm the data landed.

Everything runs in-process against a `MiniAccumuloCluster` — no external cluster,
no Spark, no MapReduce.

## 2. Scope

**In scope**

- The five steps above.
- A hardcoded 1→3 fan-out (one source entry → one entry in each of three target tables).
- A single migration pass.
- A trivially swappable demo event model.

**Out of scope** (deliberately cut to keep the PoC minimal — an earlier, larger
design covered these)

- Spark / MapReduce as the execution engine.
- Multiple migration "waves" and progressive source deletion.
- Consistency verification (counts, round-trip, referential integrity, checksums).
- Wave report generation.
- Aligning produced RFiles to target-table split points.
- Source-timestamp preservation / idempotency guarantees (see §8).
- Performance measurement at scale.

## 3. Tech stack

| Component | Version | Notes |
|-----------|---------|-------|
| Scala     | 3.3.3   | LTS |
| SBT       | 1.9.9   | `project/build.properties` |
| Accumulo  | 2.1.2   | `accumulo-core` + `accumulo-minicluster` |
| Hadoop    | 3.3.6   | `hadoop-common`; matches Accumulo 2.1.2's baseline |
| Java      | 11+     | 17 recommended (avoids module-access warnings from mini-Accumulo) |

## 4. Event model

The demo model is intentionally tiny and lives in a single file,
`model/Event.scala`:

```scala
case class Event(userId: String, eventType: String, timestamp: Long, payload: String)
```

Serialization is isolated in `EventSerializer` (same file) with three methods:
`toKey`, `toValue`, `fromEntry`.

**To change the model:** edit the `Event` case class fields and update those
three `EventSerializer` methods. Nothing else in the pipeline depends on the
field shape — `RFileReader`, `RFileWriter`, `BulkImporter`, and `TableVerifier`
all operate on raw `(Key, Value)` pairs. `Transformer` is the only other place
that names event fields, and only if the *fan-out schema* changes.

## 5. Source table

`events_source` — populated by `SourceWriter` via a `BatchWriter`, then compacted
so the data is flushed into RFiles on disk.

| Component | Value |
|-----------|-------|
| Row ID    | `userId` |
| CF        | `eventType` |
| CQ        | `timestamp` |
| Value     | `payload` |

The `BatchWriter` here is **setup only** — it stands in for "RFiles already
written to a source table." The migration itself never writes through a
TabletServer.

## 6. Fan-out rules (1 → 3)

For each source `(Key, Value)`, `Transformer.transform` emits one entry into each
of three target tables:

| Target table      | Row ID                              | CF          | CQ          |
|-------------------|-------------------------------------|-------------|-------------|
| `events_by_user`  | `userId`                            | `eventType` | `pad19(ts)` |
| `events_by_type`  | `eventType`                         | `userId`    | `pad19(ts)` |
| `events_timeline` | `pad19(Long.MaxValue - ts)`         | `userId`    | `eventType` |

`events_timeline` uses a reversed timestamp so the newest events sort first.
The fan-out is hardcoded — adjusting it means editing `Transformer` directly.

## 7. Component flow

```
            MiniAccumuloCluster (in-process)
                      │
  SourceWriter ───────┤  BatchWriter → events_source → compact() → RFiles
                      │
  RFileLocator ───────┤  scan accumulo.metadata → RFile URIs for events_source
                      │
  RFileReader ────────┤  RFile.newScanner() → Seq[(Key, Value)]   (no TabletServer)
                      │
  Transformer ────────┤  each entry → 3 (tableName, Key, Value) tuples
                      │
  RFileWriter ────────┤  sort by Key, RFile.newWriter() → one .rf per target table
                      │
  BulkImporter ───────┤  importDirectory().to(table).load()  ×3
                      │
  TableVerifier ──────┘  scan each target table → stdout
```

`Main.scala` orchestrates these in order.

| File | Responsibility |
|------|----------------|
| `Main.scala`              | Orchestrates the whole pipeline; owns the mini-cluster lifecycle |
| `model/Event.scala`       | `Event` case class + `EventSerializer` (the model seam) |
| `setup/SourceWriter.scala`| Creates `events_source`, writes events, compacts to RFiles |
| `read/RFileLocator.scala` | Finds RFile URIs for a table via the `accumulo.metadata` table |
| `read/RFileReader.scala`  | Reads `(Key, Value)` pairs from RFiles via `RFile.newScanner()` |
| `transform/Transformer.scala` | 1→3 fan-out logic |
| `write/RFileWriter.scala` | Sorts entries, writes one RFile per target table via `RFile.newWriter()` |
| `ingest/BulkImporter.scala` | Creates a target table and bulk-imports its RFile directory |
| `verify/TableVerifier.scala` | Scans a target table and prints its rows |

## 8. Key construction

`pad19(n)` = `String.format("%019d", n)` — fixed-width, zero-padded, so numeric
values sort correctly under Accumulo's lexicographic key ordering.

`events_timeline` row IDs use `pad19(Long.MaxValue - timestamp)` so a later event
produces a *smaller* row ID and therefore sorts first (reverse-chronological).

**Timestamps on produced entries.** The PoC does *not* carry the source KV
timestamp onto the produced keys — target entries get Accumulo's default
timestamp at write time. This keeps the demo simple. The consequence is that
re-running the migration is **not** guaranteed byte-identical / idempotent.
Preserving the source timestamp (passing `sourceKey.getTimestamp` into the `Key`
constructor in `Transformer`) is the change to make if idempotency is needed.

## 9. How to run

**Prerequisites:** SBT 1.9.9 and a JDK (11+, 17 recommended).

```bash
# install SBT if missing:
curl -L https://github.com/sbt/sbt/releases/download/v1.9.9/sbt-1.9.9.tgz \
  | sudo tar xz -C /usr/local/ && sudo ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt

# run the full pipeline:
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 sbt run
```

Expected output: a `[setup] … [done]` trace, the located source RFile path(s),
per-step entry counts, and a dump of all three target tables (5 rows each for the
default 5-event dataset).

The demo dataset is the five `Event` literals in `Main.scala` — edit them there.

## 10. Invariants

1. **The read step must not touch a TabletServer.** `RFileReader` uses only
   `RFile.newScanner()`; it must never import `BatchScanner` or `AccumuloClient`.
   (`RFileLocator` *does* use the client — but only to read metadata, which is a
   driver-side concern, not part of the transformation.)
2. **RFile entries must be sorted before writing.** `RFileWriter` sorts by
   `Key.compareTo()`; `RFile.newWriter().append()` throws on out-of-order keys.
3. **`fork := true` in `build.sbt` is load-bearing.** `MiniAccumuloCluster` needs
   a forked JVM to resolve its classpath correctly.
