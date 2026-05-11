# PoC Accumulo RFile Migration — Implementation Plan

Living handoff document for incremental implementation. Update it as phases land.

## Current status (2026-05-11)

**✅ Done**
- Maven scaffold (`pom.xml`, `src/main/{java,resources}`, `src/test/java`, `scripts/`, `.gitignore`).
- `application.conf` + `logback.xml` mirroring architecture §8.
- `PoCMain` entry point (stub: loads config, logs TODO).
- `SmokeTest` (verifies config loads).
- `mvn clean package` green; shaded jar at `target/accumulo-rfile-migration-1.0.0-SNAPSHOT-shaded.jar`.
- **Phase 1 — UT-6 ArchUnit rule** (`src/test/java/org/example/poc/migration/transform/ClientBypassArchTest.java`). Two rules scoped to `org.example.poc.migration.transform..`: forbids any dependency on `org.apache.accumulo.core.client..` / `clientImpl..`, and (defense-in-depth) bans the four named types `Scanner`/`BatchScanner`/`BatchWriter`/`AccumuloClient` by FQN. Uses `ImportOption.DoNotIncludeJars` so it does not try to import Spark/Hadoop jars. `allowEmptyShould(true)` so it passes vacuously while `transform/` is empty. Verified to bite: a throwaway class importing `AccumuloClient` failed both rules with a clear diagnostic before being removed.

**⏳ Not yet implemented** — phases 2–13. Components `EnvironmentSetup`, `DatasetGenerator`, `SourceRFileLocator`, `MigrationJob`, `EventTransformer`, `TargetTablePartitioner`, `BulkImporter`, `ConsistencyVerifier`, `SourceCleaner`, `WaveOrchestrator` still do not exist as source files.

## Decisions locked in by the scaffold

- **Language**: Java 17.
- **Build**: Maven, single module. NFR-4 enforced at **package** level via ArchUnit (see UT-6).
- **Versions**: Accumulo 2.1.2, Spark 3.5.3 (Scala 2.13), Hadoop 3.3.6, Jackson 2.15.2, JUnit 5, ArchUnit 1.2.1.
- **Package root**: `org.example.poc.migration`.
- **Shaded jar classifier**: `-shaded` (via `shade-plugin` classifier, not `finalName`).

## Decisions still open

Only one remaining from `docs/README.md`. Confirm before phase 5/6:

- **Wave split strategy**: `userIdMedian` (proposed in architecture §5.5) vs `byTabletRange`. The proposed default works for the synthetic dataset; `byTabletRange` only matters when source row distribution doesn't align with userId lexicographic order. **Recommendation: ship `userIdMedian` first, add `byTabletRange` only if needed.**

## Implementation phases

Phases are ordered so each one is independently testable. A phase is **done** when the listed unit/integration tests pass and the build is green.

### Phase 1 — Wire UT-6 (ArchUnit rule) first ✅

**Goal:** lock down NFR-4 enforcement before any `transform/` code exists. The rule passes vacuously now and starts biting the moment someone adds a class to `transform/`.

**Files:**
- `src/test/java/org/example/poc/migration/transform/ClientBypassArchTest.java`

**Acceptance:** UT-6 — ArchUnit rule scoped to `org.example.poc.migration.transform..` forbids:
- types under `org.apache.accumulo.core.client..`
- types under `org.apache.accumulo.core.clientImpl..`
- `Scanner`, `BatchScanner`, `BatchWriter`, `AccumuloClient` (anywhere)

Rule template (sketch — adapt to ArchUnit API):
```java
@AnalyzeClasses(packages = "org.example.poc.migration.transform")
class ClientBypassArchTest {
  @ArchTest
  static final ArchRule no_accumulo_client = noClasses()
      .should().dependOnClassesThat().resideInAnyPackage(
          "org.apache.accumulo.core.client..",
          "org.apache.accumulo.core.clientImpl..");
}
```

**Gotcha:** ArchUnit's default classpath import will choke on Spark/Hadoop deps. Use `ImportOption.DoNotIncludeJars` and target only the project's compiled classes.

---

### Phase 2 — Domain types + util

**Goal:** pure-Java building blocks with no Accumulo/Spark dependencies. Fast to test.

**Files:**
- `data/Event.java` — record with the 8 fields from data-model §2 (`eventId`, `userId`, `sessionId`, `eventType`, `timestamp`, `ipAddress`, `userAgent`, `resourceAccessed`).
- `data/EventSerializer.java` — Jackson `ObjectMapper` wrapped in a thin class. **Determinism: configure `MapperFeature.SORT_PROPERTIES_ALPHABETICALLY` or use an explicit ordered field list.** Otherwise round-trip will not be byte-identical (NFR-3).
- `util/KeyUtils.java` — `pad(long)` returns `String.format("%019d", n)`; `reverseTs(long)` returns `pad(Long.MAX_VALUE - ts)`; helpers for `yyyyMMdd(ts)` (UTC zone, locked).
- `util/Hashing.java` — SHA-256 over `byte[]`, plus `hashSortedList(Iterable<String>)` for CC-4.

**Tests:**
- UT-3 — `KeyUtils`: `reverseTs(t1) > reverseTs(t2)` lexicographically when `t1 < t2`.
- Serializer round-trip: `deserialize(serialize(event)).equals(event)` AND `serialize(serialize.deserialize(json)).equals(json)` byte-for-byte.

**Gotcha:** `yyyyMMdd` must use a fixed timezone (UTC). Using the system default will make output non-deterministic across machines (NFR-3 violation).

---

### Phase 3 — EventTransformer (the 1→7 fan-out)

**Goal:** the core transformation rule from data-model §5. Pure function from one source `KeyValue` to seven `(targetTable, Key, Value)` tuples.

**Files:**
- `transform/EventTransformer.java` — single method `List<TargetEntry> transform(Key sourceKey, Value sourceValue)`.
- `transform/TargetEntry.java` — record `(String targetTable, Key key, Value value)`.

**Tests:**
- UT-1 — fan-out shape: exactly 7 entries, distributed 1/1/1/3/1.
- UT-2 — idempotency: `transform(e).equals(transform(e))` byte-for-byte across two invocations.

**Gotcha:** preserve the source `KeyValue` timestamp on every produced `Key` (architecture §5.4). It is what makes re-import idempotent.

**NFR-4 check:** the class must not import anything under `org.apache.accumulo.core.client..`. UT-6 will fail the build if it does.

---

### Phase 4 — TargetTablePartitioner + RFile round-trip

**Goal:** the Spark `Partitioner` that aligns output partitions to target-table splits (architecture §5.2). Plus a sanity test that we can actually read/write RFiles via the static API.

**Files:**
- `transform/TargetTablePartitioner.java` — given a list of split points per target table, returns the partition index for a Key.
- `transform/RFileIO.java` — thin wrapper for `RFile.newReader()` / `RFile.newWriter()` over a Hadoop `Path` + `FileSystem`. Belongs in `transform/` (client-free).

**Tests:**
- UT-4 — partitioner: for a known split list, each Key lands in the correct partition.
- UT-5 — RFile round-trip: write N sorted `(Key, Value)` pairs to a local RFile, read them back, assert the same N pairs in the same order.

**Gotcha:** RFile writers throw if keys are not appended in ascending order (architecture §5.3). The test should verify the throw on out-of-order input, not just the happy path.

---

### Phase 5 — Setup layer: EnvironmentSetup + DatasetGenerator

**Goal:** can stand up MiniAccumulo and load a deterministic synthetic dataset.

**Files:**
- `env/EnvironmentSetup.java` — start/stop `MiniAccumuloCluster`, expose `AccumuloClient`, create the 6 tables, apply split points (computed by `DatasetGenerator`).
- `data/DatasetGenerator.java` — generates N events with a seeded `Random`, writes via `BatchWriter`. Exposes `splitPoints(targetTable)` for each of the 5 target tables, computed from the actual generated distribution.

**Tests:**
- IT-1 — after generation, `events_legacy` contains exactly N entries.

**Gotcha:** `MiniAccumuloCluster` startup is slow (30–60s) and flaky if `/tmp/mini-accumulo-poc` is dirty. `run-poc.sh` already cleans it, but integration tests need their own per-test temp dir.

---

### Phase 6 — SourceRFileLocator

**Goal:** discover the physical RFile paths of the source table portion to migrate. Uses the Accumulo client (allowed outside `transform/`).

**Files:**
- `locate/SourceRFileLocator.java` — implements Strategy A from architecture §2.3 (read `accumulo.metadata` for tablet file paths after forced `compact()`).

**Gotcha:** in MiniAccumulo the metadata table is local; paths are filesystem paths, not HDFS URIs. The Spark job needs to handle that — pass paths through as strings, let `FileSystem.get(Configuration)` resolve them.

---

### Phase 7 — MigrationJob (the Spark job)

**Goal:** wire `EventTransformer` + `TargetTablePartitioner` + `RFileIO` into a Spark job in `local[*]` mode. This is the single hardest phase.

**Files:**
- `transform/MigrationJob.java` — the Spark driver-side wiring.

**Approach (from architecture §2.4):**
1. Driver receives `List<Path>` of source RFiles + `Map<String, List<Text>>` of split points per target table (broadcast).
2. `JavaSparkContext.parallelize(rfilePaths).flatMap(...)` reads each RFile via `RFile.newReader()` and emits `(Key, Value)` pairs.
3. `flatMap` applies `EventTransformer.transform` → 7 entries per source.
4. `keyBy(targetTable).groupByKey()` or equivalent — one partition stream per target table.
5. For each target table: repartition with `TargetTablePartitioner`, `sortWithinPartitions(Key natural order)`, `mapPartitions(write one RFile via RFileIO.newWriter)`.
6. Output goes to `staging/<targetTable>/part-XXXXX.rf`.

**Tests:** none new — exercised end-to-end by IT-2.

**Gotchas:**
- **Spark + Accumulo class loaders fight.** Spark's task closures need `Key`, `Value`, `Event`, and `EventTransformer` serializable. `Key` and `Value` implement `Writable` — wrap them carefully or use Kryo registration.
- **Broadcast everything large.** Split points fit easily; don't try to broadcast `AccumuloClient` (and don't even hold a reference to one inside a closure — UT-6 will catch the import, but it's worth being mindful).
- **Don't introduce randomness or `System.currentTimeMillis()`** anywhere in the closure (NFR-3).

---

### Phase 8 — BulkImporter

**Goal:** register the staging RFiles into target tables.

**Files:**
- `ingest/BulkImporter.java` — for each of the 5 target tables, calls `client.tableOperations().importDirectory(stagingPath).to(table).tableTime(false).load()`. Sequential; returns per-table outcome. On failure of table N, reverses 1..N-1 via `deleteRows` over the imported ranges (risk 6.2).

**Gotcha:** `importDirectory` consumes the staging dir — files are moved into the table's directory by Accumulo, not copied. Don't assume staging files exist after a successful import.

---

### Phase 9 — ConsistencyVerifier

**Goal:** implement the four checks from test-plan §4. Producer of `VerificationReport`.

**Files:**
- `verify/ConsistencyVerifier.java` — implements CC-1..CC-4.
- `verify/VerificationReport.java` — record, Jackson-serializable, matching the JSON shape in test-plan §6.

**Optimizations to keep in mind:**
- CC-3 should materialize the `events_by_id` eventId set once and do set membership lookups, not per-row Accumulo lookups.
- CC-4: stream the eventIds in sorted order (`Scanner` natural order is already sorted by row, which is the eventId in `events_by_id`).

---

### Phase 10 — SourceCleaner

**Goal:** after a verified wave, delete the migrated row range from `events_legacy` and force compaction to reclaim space.

**Files:**
- `clean/SourceCleaner.java` — wraps `client.tableOperations().deleteRows(table, start, end)` + `compact(table, range, flush=true, wait=true)`.

**Gotcha:** measure dir size **before** `deleteRows` AND **after** the compaction completes, not in between (test-plan §5 disk-footprint metrics). Tombstones don't free space until major compaction + GC.

---

### Phase 11 — WaveOrchestrator + PoCMain wiring

**Goal:** glue everything together. Two waves, each running the full pipeline.

**Files:**
- `orchestrate/WaveOrchestrator.java` — runs one wave end-to-end: locate → transform → import → verify → clean → emit report.
- `orchestrate/PoCMain.java` (replace stub) — owns the lifecycle: setup, generate, loop over waves, final verification, teardown.

**Acceptance:**
- IT-2 — single wave end-to-end passes.
- IT-3 — two sequential waves: source empty at end, target counts correct.
- IT-4 — re-running a wave is a no-op (idempotency).

---

### Phase 12 — Integration tests + IT-5

**Goal:** complete test-plan §3 coverage.

**Files:**
- Tests under `src/test/java/org/example/poc/migration/it/`.
- IT-5 (failure injection) is optional per test-plan §3.

---

### Phase 13 — Polish

- Ensure all wave-report JSON fields from test-plan §6 are populated.
- Verify `scripts/run-poc.sh` produces the runbook §5 log shape.
- Verify all AC-1..AC-7 from `docs/01-requirements.md` are demonstrably met by the final run.

## How to resume in a fresh Claude session

1. Read `CLAUDE.md` first — the load-bearing invariants are non-negotiable.
2. Read this file (`docs/06-implementation-plan.md`) to find the next unticked phase.
3. Read the doc that phase implements (data-model §5 for phase 3, architecture §5.2 for phase 4, etc.).
4. Before coding: confirm any "decisions still open" relevant to that phase.
5. After coding: tick the phase here and re-run `mvn clean package` to keep the build green.

## First PR to make (concrete starting point)

**Phase 1 — wire UT-6 ArchUnit rule.** It is the smallest unit of work that protects NFR-4 from the start, and it shapes how everyone writes the `transform/` package thereafter. Create `src/test/java/org/example/poc/migration/transform/ClientBypassArchTest.java`, run `mvn test`, confirm it passes vacuously, commit.
