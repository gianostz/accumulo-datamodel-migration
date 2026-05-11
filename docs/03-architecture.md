# PoC Accumulo RFile Migration — Architecture and Design

## 1. Overview

The PoC is composed of 4 macro-components:

```
┌─────────────────────────────────────────────────────────────────┐
│  1. SETUP                                                        │
│     • Start MiniAccumuloCluster                                  │
│     • Create source table events_legacy + 5 target tables        │
│     • Configure split points on target tables                    │
│     • Generate and load synthetic dataset                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. TRANSFORMATION (per wave)                                    │
│     • Forced compaction of the source table portion             │
│     • Identify RFiles to migrate (metadata read)                │
│     • Spark job: reads RFiles, transforms, writes 5 RFile sets  │
│       (NO Accumulo client use here)                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. BULK IMPORT (per wave)                                       │
│     • importDirectory() for each of the 5 target tables         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. VERIFICATION + CLEANUP (per wave)                            │
│     • Counts, round-trip, integrity, checksum                   │
│     • If OK: deleteRows() on the processed range in events_legacy│
│     • Generate wave report                                       │
└─────────────────────────────────────────────────────────────────┘
```

Steps 2–4 are repeated for each wave (2 waves in the PoC).

## 2. Components

### 2.1 `EnvironmentSetup`
Starts the MiniAccumuloCluster, manages its lifecycle (start/stop), and exposes an `AccumuloClient` for administrative operations. Minimal configuration: 1 tablet server, root authentication with a fixed password.

### 2.2 `DatasetGenerator`
Generates N synthetic events and writes them to `events_legacy` via `BatchWriter`. Generation is seedable for reproducibility.

Configurable parameters:
- total number of events
- number of distinct users
- number of distinct sessions
- temporal distribution (timestamp range)
- generator seed

### 2.3 `SourceRFileLocator`
Utility component for identifying the physical RFiles of a portion of the source table. Implements two strategies:

**Strategy A — Via metadata table**: queries `accumulo.metadata` with `MetadataTable.NAME` to obtain RFile paths for a given tablet range. More representative of a production workflow.

**Strategy B — Via offline + dump**: takes a portion of the table offline, then enumerates its files from the filesystem. Simpler but more intrusive.

For the PoC, **Strategy A** is adopted in "read-only" mode (no offline), but forcing a preventive `compact()` to obtain a stable, consolidated set of RFiles.

### 2.4 `MigrationJob` (Spark)
Core of the PoC. A Spark job in local mode that:

1. Receives as input the list of HDFS paths (or local files, in MiniAccumulo) of the source RFiles to process in the current wave.
2. For each RFile, creates an `RFile.newScanner()` and emits `KeyValue` pairs into a distributed dataset.
3. Applies a `flatMap` that, for each source `KeyValue`, produces a set of `(targetTable, newKey, newValue)` tuples according to the transformation rules (see Data Model Document).
4. Partitions the dataset by `targetTable`.
5. For each target table:
   - Retrieves the table's split points (passed as a parameter to the job, read by the driver).
   - Repartitions and sorts by splits (custom `Partitioner`).
   - Writes one RFile per partition via `RFile.newWriter()`, under the `staging/<targetTable>/` directory.

**Critical constraint**: inside Spark `flatMap` / `mapPartitions`, the code has **no** access to any `AccumuloClient` instance. It works only with:
- `RFile.newReader()` and `RFile.newWriter()` APIs (static, operating on Hadoop `FileSystem`)
- Pure transformation logic (JSON deserialization, construction of new `Key` objects)

### 2.5 `BulkImporter`
Executed by the driver (not by Spark executors). For each of the 5 target tables, calls:

```java
client.tableOperations()
      .importDirectory(stagingPath)
      .to(targetTable)
      .tableTime(false)   // preserves RFile timestamps
      .load();
```

Atomic per individual table.

### 2.6 `ConsistencyVerifier`
Executes the 4 verifications defined in FR-6 of the Requirements Document. Uses `Scanner`/`BatchScanner` from the Accumulo client (permitted here, as we are outside the transformation phase). Produces a serializable `VerificationReport` object.

### 2.7 `SourceCleaner`
After a successful verification, deletes the row range of the source table corresponding to the just-migrated wave via `client.tableOperations().deleteRows(tableName, startRow, endRow)`. Optionally forces a compaction immediately afterward to reclaim physical space.

### 2.8 `WaveOrchestrator`
Coordinates the execution of a single wave (components 2.3 → 2.7) and produces the wave report. The PoC runs 2 waves sequentially.

## 3. Detailed Data Flow (Single Wave)

```
                    accumulo.metadata
                          │
                          │ query RFile paths
                          ▼
              ┌─────────────────────┐
              │ SourceRFileLocator  │
              └─────────────────────┘
                          │
                          │ List<Path> rfilePaths
                          ▼
              ┌─────────────────────┐
              │  Spark Driver       │
              │  (reads target      │
              │   table split pts)  │
              └─────────────────────┘
                          │
                          │ broadcast split points
                          ▼
              ┌─────────────────────────────────────────────┐
              │  Spark Executors (local[*])                  │
              │                                              │
              │  rfile1 ──┐                                  │
              │  rfile2 ──┼─► RFile.newReader().scan()       │
              │  rfileN ──┘            │                     │
              │                        ▼                     │
              │            ┌──────────────────────┐          │
              │            │ flatMap: transform   │          │
              │            │ 1 event → 7 entries  │          │
              │            └──────────────────────┘          │
              │                        │                     │
              │                        ▼                     │
              │            ┌──────────────────────┐          │
              │            │ partitionBy:         │          │
              │            │ (targetTable, key)   │          │
              │            └──────────────────────┘          │
              │                        │                     │
              │                        ▼                     │
              │            ┌──────────────────────┐          │
              │            │ sortWithinPartition  │          │
              │            └──────────────────────┘          │
              │                        │                     │
              │                        ▼                     │
              │            ┌──────────────────────┐          │
              │            │ RFile.newWriter()    │          │
              │            │ per partition        │          │
              │            └──────────────────────┘          │
              └─────────────────────────────────────────────┘
                          │
                          │ staging/<table>/part-*.rf
                          ▼
              ┌─────────────────────┐
              │  BulkImporter       │
              │  (5x importDirectory)│
              └─────────────────────┘
                          │
                          ▼
              ┌─────────────────────┐
              │ ConsistencyVerifier │
              └─────────────────────┘
                          │
                          │ OK
                          ▼
              ┌─────────────────────┐
              │  SourceCleaner      │
              │  (deleteRows)       │
              └─────────────────────┘
```

## 4. Technology Choice: Spark vs MapReduce

This section documents the rationale for the choice required by NFR-5.

### 4.1 Apache Spark — Advantages in Our Context

**More expressive programming model**. The 1→N transformation logic with partitioning by destination table is expressed in Spark in a few dozen lines using `flatMap` + `partitionBy` + `mapPartitions`. In MapReduce this would require MultipleOutputs and a custom Reducer for each table.

**In-memory and efficient fan-out**. The 1→7 fan-out (one source event produces 7 entries across 5 tables) benefits from in-memory pipelining: Spark does not write intermediate `flatMap` results to disk, while MapReduce would materialize Mapper output before the shuffle.

**Local mode**. Spark runs in-process as `local[*]`, ideal for a PoC on MiniAccumuloCluster on a single machine. MapReduce requires `LocalJobRunner`, which is less integrated, slower, and less representative of a real cluster.

**Ecosystem**. Spark integrates well with HDFS, and the API for custom partitioning on non-standard sources (such as RFiles) is well supported. Community accumulo-spark integration projects exist that can speed up development.

**DataFrames are not useful here**. It must be said: the main advantage of modern Spark (Catalyst, Tungsten, DataFrame API) **does not apply** to our case, because we work with Accumulo `Key/Value` objects that have no natural relational schema and require low-level RDDs. Spark is still the right choice, but for the above reasons — not because of Catalyst.

### 4.2 MapReduce — Advantages in Other Contexts

**Maturity within the Accumulo ecosystem**. Official Accumulo documentation for bulk import historically uses MapReduce (`AccumuloFileOutputFormat` is native to MR). The patterns are codified and well documented.

**Abundant sample code**. Examples of "RFile ingest via MR job" are well represented in the Accumulo repository (`accumulo-examples`).

**Predictable memory footprint**. MapReduce has a much more predictable execution model (one task = one JVM, sorted output to disk, deterministic shuffle), useful in production scenarios with strict resource constraints.

**Stability at very large volumes**. At petabyte scale and on clusters with limited resources, MapReduce sometimes outperforms Spark due to its disk-based model that does not accumulate in-memory state.

### 4.3 Summary Comparison Table

| Aspect                                  | Spark (local mode)          | MapReduce (LocalJobRunner) |
|-----------------------------------------|-----------------------------|----------------------------|
| Single-node PoC setup                   | Simple                      | More verbose               |
| Expressiveness of 1→N transformation    | High (flatMap + partitionBy) | Medium (MultipleOutputs)  |
| Intermediate shuffle cost               | In-memory (low)             | Disk (higher)              |
| Maturity with Accumulo                  | Good but less documented    | Excellent, official examples|
| Code reusability for production         | High (same code on cluster) | High (same)                |
| Learning curve                          | Low for those familiar with Scala/Java + collection API | Medium |
| Memory footprint on single-node         | Higher                      | Lower                      |
| PoC development time                    | Faster                      | Slower                     |

### 4.4 Conclusion

**Spark in local[*] mode is chosen** for the PoC, with the following dominant motivations:

1. **Expressiveness of the transformation code**: the 1→N fan-out logic with partitioning by destination table is written in a much more compact and readable way.
2. **Development time**: the PoC aims to demonstrate the pattern, not to optimize for edge cases. Spark reduces time-to-PoC.
3. **Representativeness for production**: if the PoC is later promoted to a production job, Spark is easily portable to Spark on YARN, Spark Standalone, or Kubernetes, with the transformation code remaining identical.
4. **In-process local mode**: integrates very naturally with MiniAccumuloCluster in the same JVM as the orchestrator, simplifying PoC setup.

MapReduce remains a valid and potentially preferable choice in production at very large volumes and with strict memory constraints; any promotion of the PoC to a production job should re-evaluate this trade-off.

## 5. Critical Design Choices

### 5.1 Bypass of the Accumulo Client During Transformation

The NFR-4 constraint (no Scanner/BatchWriter in the transformation) is implemented as follows:

- **Reading**: use `org.apache.accumulo.core.file.rfile.RFile.newScanner()` (2.1.x signature). This API is static, accepts the RFile path, a Hadoop `FileSystem`, and a `Configuration` as parameters. It opens no connection to the cluster.
- **Writing**: similarly, `RFile.newWriter()` accepts path + FileSystem + Configuration. Accumulo `Key` and `Value` objects are simple serializable POJOs and do not require the client.
- **Architectural boundary**: the project is a **single Maven module** (see §7). The bypass is enforced at the **package** level: classes in `org.example.poc.migration.transform.*` (executed inside Spark `flatMap` / `mapPartitions`) must not depend on `org.apache.accumulo.core.client.*`, `org.apache.accumulo.core.clientImpl.*`, or any `Scanner` / `BatchScanner` / `BatchWriter` / `AccumuloClient` type. This is enforced by an ArchUnit rule wired into UT-6, which fails the build if a forbidden import appears in any `transform/*` class.

The Accumulo client APIs are used only by classes outside the `transform/` package (setup, bulk import, verification, cleanup), invoked from the driver — never from executor-side code.

### 5.2 Spark Partitioning Aligned to Target Table Splits

For each target table, the driver reads split points with `client.tableOperations().listSplits(tableName)`. The splits are passed as broadcast variables to the executors.

A custom Spark `Partitioner` is implemented that, given an Accumulo `Key`, computes the correct partition according to the splits. The number of partitions per target table = (number of splits) + 1.

**Rationale**: without this alignment, each produced RFile might contain keys belonging to multiple tablets of the target table. At bulk import time, Accumulo would have to **split each RFile on the fly** to align it to tablets — an expensive operation that defeats the purpose of bulk import. With aligned partitioning, each produced RFile falls entirely within one tablet → efficient bulk import.

### 5.3 Key Ordering in Produced RFiles

Accumulo RFiles require that keys be written in **ascending order** according to the native `Key` comparator (row, cf, cq, vs, ts descending). The Spark job applies `sortWithinPartitions` on each partition before writing.

If the ordering is violated, `RFile.newWriter().append()` throws an exception. This invariant is therefore automatically verified.

### 5.4 Timestamps in Produced RFiles

`KeyValue` entries written to the target tables use the **same timestamp** as the source KeyValue. This:
- Preserves temporal semantics (Accumulo versioning).
- Guarantees idempotency on re-import (the identical timestamp means entries are "equivalent").
- Allows distinguishing migrated data from data newly ingested after migration (new data will have more recent timestamps).

Alternatively, a fixed "migration" timestamp could be used to facilitate selective rollback (see section 6.3).

### 5.5 Two-Wave Strategy

The generated dataset (e.g., 10,000 events) is partitioned into 2 halves by `userId`:

- **Wave 1**: users whose userId sorts lexicographically before the median value (e.g., `user-000` ... `user-049`).
- **Wave 2**: users from the median onward.

Between waves, the corresponding range is deleted from the source table. This concretely demonstrates that progressive deletion works and frees space (verifiable via `du` on the Accumulo data directory, after compaction).

## 6. Risks and Mitigations

### 6.1 RFiles Produced with Unordered Keys

**Risk**: Spark's `sortWithinPartitions` sorts according to the natural object comparator, which for Accumulo `Key` is already correct — but it is easy to get the partitioning key wrong and end up with an incorrect sort.

**Mitigation**: the Spark key is `(targetTable, Key)`; the partitioner uses only `targetTable` to choose the partition, but the internal sort uses the natural ordering of `Key`. Dedicated unit test.

### 6.2 Partial Bulk Import Failure (One or More of 5 Tables)

**Risk**: if the import for table 3 fails after tables 1 and 2 have succeeded, the state is inconsistent.

**Mitigation in the PoC**: bulk import is executed sequentially and each result is evaluated. On failure after the first, the wave is aborted, already successful imports are **reversed** with `deleteRows` on the imported range (identifiable since it is known), and the entire wave is retried.

In production, a more sophisticated rollback mechanism will be evaluated (e.g., using a "batch ID" attribute in the Keys to uniquely identify entries of a batch and delete them in case of rollback).

### 6.3 Compromised Idempotency

**Risk**: if the transformation is not perfectly idempotent, a retry produces duplicates.

**Mitigation**: section 7 of the Data Model Document defines the transformation as deterministic. Unit test: the same input applied twice → byte-identical output. Also, all target rowIds include the `eventId`, ensuring that any duplicates are simply overwritten (Accumulo keeps the latest version for the same key, and with an identical timestamp there are no additional versions either).

### 6.4 Deletion That Does Not Free Space

**Risk**: `deleteRows` writes tombstones; space is not reclaimed until major compaction + GC.

**Mitigation**: after each `deleteRows`, force `client.tableOperations().compact(table, range, true, true)` (flush + wait). Verify in the wave report that the on-disk size of the `events_legacy` directory has actually decreased.

### 6.5 Split Points Not Representative of the Dataset

**Risk**: with wrong split points, Spark partitions are unbalanced and/or produced RFiles cover multiple tablets.

**Mitigation**: split points are calculated by the `DatasetGenerator` based on the actual distribution of the generated data (e.g., medians on key fields). Setup installs them on the target tables before import.

## 7. Project Structure

The PoC is a **single Maven module** (not multi-module). The `transform/` boundary required by NFR-4 is enforced at the **Java package** level by an ArchUnit rule in UT-6 — see §5.1.

```
poc-migration/
├── pom.xml
├── README.md
├── CLAUDE.md
├── docs/
│   ├── 01-requirements.md
│   ├── 02-data-model.md
│   ├── 03-architecture.md
│   ├── 04-test-plan.md
│   └── 05-runbook.md
├── src/main/java/org/example/poc/migration/
│   ├── env/        EnvironmentSetup.java
│   ├── data/       DatasetGenerator.java, Event.java, EventSerializer.java
│   ├── locate/     SourceRFileLocator.java
│   ├── transform/  MigrationJob.java, EventTransformer.java, TargetTablePartitioner.java
│   ├── ingest/     BulkImporter.java
│   ├── verify/     ConsistencyVerifier.java, VerificationReport.java
│   ├── clean/      SourceCleaner.java
│   ├── orchestrate/WaveOrchestrator.java, PoCMain.java
│   └── util/       Hashing.java, KeyUtils.java
├── src/main/resources/
│   ├── application.conf
│   └── logback.xml
├── src/test/java/org/example/poc/migration/  unit and integration tests (including ArchUnit rule for NFR-4)
└── scripts/
    ├── run-poc.sh
    ├── cleanup.sh
    └── inspect-results.sh
```

**Why single-module, not multi-module:** a Maven module split (a separate `transform` artifact declaring only `accumulo-core`) would give compile-time enforcement of NFR-4 instead of a test-time rule. We chose single-module because (a) the PoC is small and a build-graph split adds overhead disproportionate to the benefit, (b) the ArchUnit rule fails the build just as effectively as a missing dependency, and (c) keeping one module makes the Spark fat-jar and the orchestrator JVM literally the same artifact, which simplifies `local[*]` execution. Promoting the PoC to production is a reasonable point to revisit this — at that point splitting `transform/` into its own module is cheap.

## 8. Configuration

All PoC parameters are in an `application.conf` (HOCON) or `application.properties` file:

```hocon
dataset {
  totalEvents = 10000
  uniqueUsers = 100
  uniqueSessions = 500
  timeRangeStartMillis = 1735689600000  # 2025-01-01
  timeRangeEndMillis   = 1738368000000  # 2025-02-01
  randomSeed = 42
}

waves {
  count = 2
  splitStrategy = "userIdMedian"  # or "byTabletRange"
}

spark {
  master = "local[*]"
  appName = "AccumuloRFileMigrationPoC"
}

accumulo {
  miniDir = "/tmp/mini-accumulo-poc"
  rootPassword = "secret"
}

paths {
  stagingBase = "/tmp/poc-staging"
  reportsDir  = "/tmp/poc-reports"
}
```

## 9. Main Dependencies (Indicative Versions)

- `accumulo-core` 2.1.2
- `accumulo-minicluster` 2.1.2
- `hadoop-client` (version compatible with Accumulo 2.1.2; 3.3.x)
- `spark-core` 3.5.x with appropriate scope
- `jackson-databind` for JSON
- `slf4j-api` + `logback-classic`
- `junit-jupiter` for tests
