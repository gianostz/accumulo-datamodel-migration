# PoC Accumulo RFile Migration — Implementation Plan

Living handoff document for incremental implementation. Update it as phases land.

## Current status (2026-05-12)

**✅ Done**
- Maven scaffold (`pom.xml`, `src/main/{java,resources}`, `src/test/java`, `scripts/`, `.gitignore`).
- `application.conf` + `logback.xml` mirroring architecture §8.
- `PoCMain` entry point (stub: loads config, logs TODO).
- `SmokeTest` (verifies config loads).
- `mvn clean package` green; shaded jar at `target/accumulo-rfile-migration-1.0.0-SNAPSHOT-shaded.jar`.
- **Phase 1 — UT-6 ArchUnit rule** (`src/test/java/org/example/poc/migration/transform/ClientBypassArchTest.java`). Two rules scoped to `org.example.poc.migration.transform..`: forbids any dependency on `org.apache.accumulo.core.client..` / `clientImpl..`, and (defense-in-depth) bans the four named types `Scanner`/`BatchScanner`/`BatchWriter`/`AccumuloClient` by FQN. Uses `ImportOption.DoNotIncludeJars` so it does not try to import Spark/Hadoop jars. `allowEmptyShould(true)` so it passes vacuously while `transform/` is empty. Verified to bite: a throwaway class importing `AccumuloClient` failed both rules with a clear diagnostic before being removed.
- **Phase 2 — Domain types + util.** `data/Event.java` (8-field record), `data/EventSerializer.java` (Jackson; `SORT_PROPERTIES_ALPHABETICALLY` + `ORDER_MAP_ENTRIES_BY_KEYS` for NFR-3 byte-stability), `util/KeyUtils.java` (`pad(long)` → 19-char `%019d`, `reverseTs(long)` = `pad(MAX_LONG - ts)`, `yyyyMMdd(long)` UTC-locked via `DateTimeFormatter.withZone(ZoneOffset.UTC)`), `util/Hashing.java` (`sha256`, `sha256Hex`, `hashSortedList` — sort + `\n`-join + SHA-256 for CC-4). Tests: `KeyUtilsTest` (UT-3 + width/UTC invariants, 8 tests), `EventSerializerTest` (object round-trip, alphabetical key order check, byte-stable round-trip from canonical JSON, repeat-serialize byte-identical — 4 tests), `HashingTest` (known-vector SHA-256 for `""` and `"abc"`, order-insensitivity, newline-join contract, empty-list fixed value, 32-byte length — 8 tests). `mvn clean test` green: 23 tests, 0 failures.
- **Phase 3 — EventTransformer (1→7 fan-out).** `transform/TargetEntry.java` (record `(String targetTable, Key key, Value value)` — uses only `org.apache.accumulo.core.data.*`, no client deps) and `transform/EventTransformer.java` (pure `transform(Key, Value) → List<TargetEntry>` implementing the data-model §5 rule table verbatim). Every produced Key carries `sourceKey.getTimestamp()` (architecture §5.4), never `event.timestamp` or `now()`. Re-serializes the event JSON for the `events_by_id` value through `EventSerializer` so non-canonical source bytes are normalized — keeps NFR-3 byte-identity invariant of input encoding. Tests in `EventTransformerTest`: UT-1 (fan-out: exactly 7, distributed 1/1/1/3/1), UT-2 (byte-for-byte idempotency across two invocations), source-timestamp-preserved (fixture uses `SOURCE_TS` ≠ `event.timestamp` to make the bug visible if the wrong ts leaks through), rowId/CF/CQ contract per §5 for all 5 target tables, and a defensive test that the `events_by_id` value is canonical re-serialization (not source pass-through). UT-6 still green: nothing in `transform/` imports `accumulo.core.client..` (only `accumulo.core.data.Key`/`Value`). `mvn clean test` green: 28 tests, 0 failures.
- **Phase 4 — TargetTablePartitioner + RFile round-trip.** `transform/TargetTablePartitioner.java` extends `org.apache.spark.Partitioner`; `numPartitions = splits.size()+1`; `getPartition(Object)` accepts a `Key`/`Text`/`String` and `Collections.binarySearch` on a sorted defensive copy of splits — matches Accumulo's inclusive-upper-bound split semantics (row equal to split S → tablet ending at S). `transform/RFileIO.java` wraps the static-file API: `openWriter(path, fs)` → `RFileWriter` (caller closes); `openReader(path, fs)` → `RFileIO.Reader` view exposing only `Iterable<Map.Entry<Key,Value>>` + `AutoCloseable`, so `transform/` callers never need to name `Scanner`. Tests: `TargetTablePartitionerTest` (10 tests — empty splits, partition-count, below/equal/between/above splits, defensive sort, Text/String acceptance, IAE on unknown type, plus a realistic `user-100/200/300` scenario); `RFileIOTest` (3 tests using `@TempDir` + local FS — N-pair round-trip, writer rejects out-of-order append per architecture §5.3, empty-file round-trip). **UT-6 carve-out** (necessary, narrow): the public static-file API in Accumulo 2.1.x lives at `org.apache.accumulo.core.client.rfile.*` and `RFile.newScanner().build()` returns `org.apache.accumulo.core.client.Scanner` (a Scanner bound to an RFile, not a cluster). `ClientBypassArchTest` was refined to exempt `client.rfile..` and the two types `client.Scanner`/`client.ScannerBase` — and only those — using a `DescribedPredicate<JavaClass>`. `BatchScanner`/`BatchWriter`/`AccumuloClient` remain banned by FQN. The Scanner reference is intentionally confined to `RFileIO` itself (callers see only `Iterable<Map.Entry<Key,Value>>`), so widening the carve-out further would require an explicit doc update. `mvn clean test` green: 41 tests, 0 failures.
- **Phase 5 — Setup layer: EnvironmentSetup + DatasetGenerator + IT-1.** `env/EnvironmentSetup.java` wraps `MiniAccumuloCluster` lifecycle (`AutoCloseable`, idempotent `start()`, closes client before stopping cluster, propagates the first failure with suppressed siblings) and exposes `client()`, `createTables()` for the six PoC tables (`events_legacy` + the five targets — `ALL_TABLES` is the canonical list, `TableExistsException` is swallowed for re-runnability), and `applySplits(table, splits)`. `data/DatasetGenerator.java` is config-driven (`fromConfig(Config)`), pure-deterministic from `randomSeed` (eventIds issued sequentially as `evt-%08x`; userId/sessionId widths derived from `uniqueUsers`/`uniqueSessions`; type/ip/resource/userAgent from fixed pools); writes via `BatchWriter` with an ingestion ts of `timeRangeEndMillis + i` (deterministic, distinct from `event.timestamp` — re-runs overwrite via Accumulo versioning, no duplicates). `splitPoints(events)` returns observed quartile splits for `events_by_id`/`events_by_user`/`events_by_session` (3 splits → 4 tablets), and the prefix-isolation splits documented in data-model §8 for `event_components_searchable` (`ip_~`, `resource_~` — `~` = 0x7E, above the printable chars used in field values) and `event_stats_by_type` (`LOGIN`, `LOGOUT`, `RESOURCE_ACCESS`, since each `<type>_…` row sorts strictly above the bare type string). **IT-1** (`it/DatasetGenerationIT.java`) — Failsafe-named so `mvn test` skips it; `@TempDir static Path tempDir` because `@BeforeAll` runs before non-static field injection; `@TestInstance(PER_CLASS)` to amortize the ~30s MiniAccumulo cycle across the four cases (row count = N, generator determinism across instances, splits actually present on each target table via `listSplits`, all six tables present). Pom changes: added `maven-failsafe-plugin` and extracted the surefire JDK17 `--add-opens` argLine to a shared `${test.argLine}` property so failsafe gets the same flags. `mvn clean verify` green: 41 UTs + 4 ITs.
- **Phase 6 — SourceRFileLocator (Strategy A).** `locate/SourceRFileLocator.java` enumerates the physical RFiles backing a table by reading `accumulo.metadata`. Surface: `compactAndWait(table)` / `compactAndWait(table, start, end)` (sync major compaction via `CompactionConfig.setFlush(true).setWait(true)` so the metadata reflects a stable file set before the read); `locate(table)` / `locate(table, startRow, endRow)` returning `List<RFileRef>` sorted by path (deterministic for Spark `parallelize`). Implementation: scans `MetadataTable.NAME` over `TabletsSection.getRange(tableId)`, fetches the `file:` column family for file entries and the `~tab:~pr` column for each tablet's `prevEndRow`, parses CQ via `new StoredTabletFile(cq).getPathStr()` and value via `new DataFileValue(value.get())`. The tablet's `endRow` is decoded from the metadata row — `<tableId>;<endRow>` for a normal tablet, `<tableId><` for the default (last) tablet (mapped to `null`). Range filtering is at tablet granularity (Strategy A is by-tablet by construction): a tablet contributes its files iff its `(prevEndRow, endRow]` overlaps the requested `[startRow, endRow]` — `<=` on the prev side keeps the lower bound open. Documented carve-out: callers needing per-row filtering must do it inside the KV pipeline, not at the locator. **IT** (`it/SourceRFileLocatorIT.java`, 4 tests, `@TestInstance(PER_CLASS)`): pre-splits `events_legacy` at `user-4`/`user-7` so range filtering is meaningfully exercised (without splits every query returns the same single file); verifies (a) the locator returns ≥1 file per tablet and the summed `numEntries` equals `TOTAL_EVENTS`, (b) each path exists on disk, the metadata size matches `FileSystem.getFileStatus`, and reading the file via `RFileIO.openReader` yields the recorded entry count — i.e. the locator's output is consumable by the Phase-7 Spark workers exactly as-is, (c) a middle-tablet range query returns a proper subset bounded to one tablet with `prevEndRow=user-4`, `endRow=user-7`, (d) a right-unbounded query (`endRow=null`) reaches the default tablet whose `endRow==null`. `mvn clean verify` green: 41 UTs + 8 ITs.
- **Phase 7 — MigrationJob (Spark driver).** `transform/MigrationJob.java` wires `SourceRFileLocator` paths → `flatMap(EventTransformer)` → per-table `repartitionAndSortWithinPartitions(TargetTablePartitioner)` → `mapPartitionsWithIndex` writing one RFile per non-empty partition under `staging/<targetTable>/part-XXXXX.rf`. Two entry points: `run(...)` creates and tears down its own `JavaSparkContext`; `runWith(jsc, ...)` uses an injected one (so ITs can amortize the slow startup across cases). Returns `TransformResult(stagingPathsByTable, entryCountsByTable)` so callers can verify CC-1 without scanning the files. **Spark + Accumulo serialization** is the meat of this phase: I confirmed by inspecting the bytecode of accumulo-core 2.1.2 that `org.apache.accumulo.core.data.Key` does **not** implement `Serializable` (only `WritableComparable` + `Cloneable` + `Comparable`); same story for `org.apache.hadoop.io.Text`. Two new types unblock the shuffle: `transform/SerializableKey.java` is an `Externalizable` wrapper that delegates `writeExternal`/`readExternal` to `Key.write`/`readFields`, exposes the underlying `Key` plus `getRow()`, and implements `Comparable<SerializableKey>` so `repartitionAndSortWithinPartitions` sorts by the natural Accumulo Key ordering (architecture §5.3). `TargetTablePartitioner` got custom `writeObject`/`readObject` that ship its `List<Text>` splits as raw UTF-8 byte arrays (the field is now `transient` — the existing API and tests are unchanged). The partitioner also gained an `instanceof SerializableKey` branch so the production-path Spark shuffle key resolves to a partition without an extra wrap/unwrap. **Per-table loop** caches the post-fan-out RDD with `MEMORY_AND_DISK` (5 filter+shuffle passes would otherwise re-read every source RFile); `Comparator<SerializableKey> & Serializable` is passed explicitly to the sort so we don't depend on Spark's implicit `Comparable` discovery. The staging directory for each table is wiped and recreated on the driver before each run — required for re-runs to be byte-equivalent (NFR-3). Empty partitions skip the writer entirely (no zero-entry RFiles littering staging). **UT-6 still green** — only the static-file RFile API + Hadoop `FileSystem` cross into `transform/`. New tests: `SerializableKeyTest` (4 cases — Java-serialization round-trip preserves all Key fields, natural ordering, descending-ts ordering quirk of Accumulo Keys, defensive copy on construction); `TargetTablePartitionerTest` extended with two cases (accepts `SerializableKey`, partition assignment survives Java serialization round-trip). **IT-MigrationJobIT** (6 cases, `@TestInstance(PER_CLASS)`, MiniAccumulo + a shared `local[2]` `JavaSparkContext`): per-table entry counts honor the 1/1/1/3/1 fan-out shape (CC-1 shape); every staging RFile exists with non-zero footer; per-file read-back is monotonically key-ordered and the count matches `TransformResult` (defense-in-depth on top of `RFile.append`'s out-of-order throw); each output RFile lies entirely within one tablet of the target table (architecture §5.2 alignment, verified by computing the partition of min/max rows in the staging file under the target table's split list — duplicated locally so the test doesn't shadow a sign bug in the production partitioner); every produced KV's timestamp falls in the source-ingest range `[TIME_END, TIME_END+TOTAL_EVENTS)` (architecture §5.4 source-ts preservation); a re-run with the same inputs produces identical per-table entry counts (NFR-3). `mvn clean verify` green: 47 UTs + 14 ITs (6 new in MigrationJobIT). End-to-end pipeline runs in ~106s on the test machine. **Gotchas hit:** (a) `Text` not being `Serializable` would have killed the partitioner shuffle silently if I had relied on default Java serialization; the bytecode-level check on `Key.class` and `Text.class` was worth the two minutes. (b) `mapPartitionsWithIndex` on a `JavaPairRDD` returns `JavaRDD`, so collecting per-partition counts needed a small `PartCount` POJO rather than a `Tuple2`. (c) Broadcasting `tableName`/`stagingDir` per loop iteration (rather than capturing the loop-local `String`) keeps the closure clean of per-iteration state and matches the pattern Phase 11 will need when it hands the same job builder to two waves back-to-back.

**⏳ Not yet implemented** — phases 8–13. Components `BulkImporter`, `ConsistencyVerifier`, `SourceCleaner`, `WaveOrchestrator` still do not exist as source files.

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

### Phase 2 — Domain types + util ✅

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

### Phase 3 — EventTransformer (the 1→7 fan-out) ✅

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

### Phase 4 — TargetTablePartitioner + RFile round-trip ✅

**Goal:** the Spark `Partitioner` that aligns output partitions to target-table splits (architecture §5.2). Plus a sanity test that we can actually read/write RFiles via the static API.

**Files:**
- `transform/TargetTablePartitioner.java` — given a list of split points per target table, returns the partition index for a Key.
- `transform/RFileIO.java` — thin wrapper for `RFile.newReader()` / `RFile.newWriter()` over a Hadoop `Path` + `FileSystem`. Belongs in `transform/` (client-free).

**Tests:**
- UT-4 — partitioner: for a known split list, each Key lands in the correct partition.
- UT-5 — RFile round-trip: write N sorted `(Key, Value)` pairs to a local RFile, read them back, assert the same N pairs in the same order.

**Gotcha:** RFile writers throw if keys are not appended in ascending order (architecture §5.3). The test should verify the throw on out-of-order input, not just the happy path.

---

### Phase 5 — Setup layer: EnvironmentSetup + DatasetGenerator ✅

**Goal:** can stand up MiniAccumulo and load a deterministic synthetic dataset.

**Files:**
- `env/EnvironmentSetup.java` — start/stop `MiniAccumuloCluster`, expose `AccumuloClient`, create the 6 tables, apply split points (computed by `DatasetGenerator`).
- `data/DatasetGenerator.java` — generates N events with a seeded `Random`, writes via `BatchWriter`. Exposes `splitPoints(targetTable)` for each of the 5 target tables, computed from the actual generated distribution.

**Tests:**
- IT-1 — after generation, `events_legacy` contains exactly N entries.

**Gotcha:** `MiniAccumuloCluster` startup is slow (30–60s) and flaky if `/tmp/mini-accumulo-poc` is dirty. `run-poc.sh` already cleans it, but integration tests need their own per-test temp dir.

---

### Phase 6 — SourceRFileLocator ✅

**Goal:** discover the physical RFile paths of the source table portion to migrate. Uses the Accumulo client (allowed outside `transform/`).

**Files:**
- `locate/SourceRFileLocator.java` — implements Strategy A from architecture §2.3 (read `accumulo.metadata` for tablet file paths after forced `compact()`).

**Gotcha:** in MiniAccumulo the metadata table is local; paths are filesystem paths, not HDFS URIs. The Spark job needs to handle that — pass paths through as strings, let `FileSystem.get(Configuration)` resolve them.

---

### Phase 7 — MigrationJob (the Spark job) ✅

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
