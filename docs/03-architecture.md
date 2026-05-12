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
4. Filters the unified RDD per target table.
5. For each target table:
   - Sorts the RDD by the natural Accumulo `Key` ordering (`sortByKey` on `(Key, Value)`).
   - Calls `JavaPairRDD.saveAsNewAPIHadoopFile(staging/<targetTable>, Key.class, Value.class, AccumuloFileOutputFormat.class, hadoopConf)`.

`AccumuloFileOutputFormat` (from `org.apache.accumulo.hadoop.mapreduce`) is a Hadoop `OutputFormat<Key, Value>` that opens one RFile per task at `getDefaultWorkFile()` and writes via the same static RFile writer the PoC used previously. The driver configures block sizes / compression / sampler on the Hadoop `Configuration` once (via `AccumuloFileOutputFormat.configure().outputPath(...).store(job)`); inside executors, `getRecordWriter()` reads only that `Configuration` — no Accumulo client, no TabletServer traffic, no Zookeeper lookup.

**Critical constraint**: inside Spark `flatMap` / `mapPartitions`, the code has **no** access to any `AccumuloClient`, `BatchScanner`, or `BatchWriter`. It works only with:
- `RFile.newReader()` (source read) and `AccumuloFileOutputFormat` (target write)
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
              │  (builds Hadoop     │
              │   Configuration via │
              │   AFOF.configure()) │
              └─────────────────────┘
                          │
                          │ Hadoop Configuration (one per table)
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
              │            │ per target table:    │          │
              │            │ filter + sortByKey   │          │
              │            └──────────────────────┘          │
              │                        │                     │
              │                        ▼                     │
              │            ┌──────────────────────────────┐  │
              │            │ saveAsNewAPIHadoopFile +     │  │
              │            │ AccumuloFileOutputFormat     │  │
              │            │ (one RFile per task)         │  │
              │            └──────────────────────────────┘  │
              └─────────────────────────────────────────────┘
                          │
                          │ staging/<table>/part-r-*.rf
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

### 4.4 Use of `AccumuloFileOutputFormat` from Spark

The write side of the transformation uses `org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat` — a Hadoop `OutputFormat` originally written for MapReduce — driven from Spark via the Hadoop OutputFormat compatibility layer (`JavaPairRDD.saveAsNewAPIHadoopFile(...)`). This is intentional: the OutputFormat is a thin, battle-tested wrapper around the static-file RFile writer that handles compression, block sizing, sampler/summariser configuration, and the Hadoop work-file / commit lifecycle correctly. Reimplementing those concerns in a Spark `mapPartitions` writer means rediscovering edge cases under incidents. Spark gives us the expressive transformation; Accumulo's `OutputFormat` gives us the well-trodden write path. The PoC keeps both.

### 4.5 Conclusion

**Spark in local[*] mode is chosen** for the PoC, with the following dominant motivations:

1. **Expressiveness of the transformation code**: the 1→N fan-out logic with partitioning by destination table is written in a much more compact and readable way.
2. **Development time**: the PoC aims to demonstrate the pattern, not to optimize for edge cases. Spark reduces time-to-PoC.
3. **Representativeness for production**: if the PoC is later promoted to a production job, Spark is easily portable to Spark on YARN, Spark Standalone, or Kubernetes, with the transformation code remaining identical.
4. **In-process local mode**: integrates very naturally with MiniAccumuloCluster in the same JVM as the orchestrator, simplifying PoC setup.

MapReduce remains a valid and potentially preferable choice in production at very large volumes and with strict memory constraints; any promotion of the PoC to a production job should re-evaluate this trade-off.

## 5. Critical Design Choices

### 5.1 No TabletServer Traffic During Transformation

NFR-4 is now scoped to TabletServer traffic, not the Accumulo client surface as a whole. Read-only metadata access (e.g. split-point queries via `client.tableOperations().listSplits()`) is permitted; cluster writes via `BatchWriter` and bulk reads via `BatchScanner` remain forbidden during transformation. The constraint is implemented as follows:

- **Reading**: use `org.apache.accumulo.core.client.rfile.RFile.newScanner()` (2.1.x signature). This API is static, accepts the RFile path, a Hadoop `FileSystem`, and a `Configuration` as parameters. It opens no connection to the cluster.
- **Writing**: use `org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat`, a Hadoop `OutputFormat<Key, Value>`. Its `getRecordWriter()` reads only the Hadoop `Configuration` (for block size, compression, sampler) and opens an RFile writer at the task's default work file. We verified by disassembling `AccumuloFileOutputFormat.getRecordWriter()` in Accumulo 2.1.2 that no `AccumuloClient` is instantiated and no Zookeeper / TabletServer connection is opened. `Key` and `Value` are simple serializable POJOs.
- **Architectural boundary**: the project is a **single Maven module** (see §7). The bypass is enforced at the **package** level: classes in `org.example.poc.migration.transform.*` (executed inside Spark `flatMap` / `mapPartitions`) must not depend on `BatchScanner`, `BatchWriter`, or `AccumuloClient`. Classes under `org.apache.accumulo.core.client..` / `clientImpl..` remain banned, with deliberate carve-outs for the static-file RFile API (`client.rfile..`, `client.Scanner`, `client.ScannerBase`). The new write-side type `org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat` is in a different package (`accumulo.hadoop..`) and therefore not in scope of the original ban — it is allowed unconditionally. The ArchUnit rule in UT-6 fails the build if a forbidden type leaks into `transform/`.

The Accumulo client APIs are used only by classes outside the `transform/` package (setup, bulk import, verification, cleanup), invoked from the driver — never from executor-side code.

### 5.2 Spark Partitioning

The PoC does **not** align Spark output partitions to target-table split points. For each target table the job:

1. Filters the unified RDD down to entries for that table.
2. Calls `sortByKey()` on the natural Accumulo `Key` ordering. This produces an RDD partitioned by Spark's sample-based `RangePartitioner` (Spark's own, not Accumulo's) — partition boundaries reflect the data distribution, not the target tablet split points.
3. Writes via `saveAsNewAPIHadoopFile(staging/<table>, ..., AccumuloFileOutputFormat.class, hadoopConf)`. Each task produces one RFile.

`AccumuloFileOutputFormat` itself does **not** handle split alignment — we verified this by disassembling Accumulo 2.1.2: its public API only exposes block size, compression, replication, sampler, and summariser configuration. Split alignment in the MapReduce world is the responsibility of a separate `KeyRangePartitioner` configured at the `Job` level, which Spark's `saveAsNewAPIHadoopFile` does not invoke.

**Consequence and rationale**: produced RFiles may cross tablet boundaries of the target table. `TableOperations.importDirectory()` accepts such RFiles and **re-splits them on the fly at import time**. This is a non-zero cost, but at PoC scale (10k–100k events) the cost is small. The PoC explicitly trades a fully aligned write path for a substantially simpler Spark job (no custom partitioner, no broadcast of split arrays, no per-table partitioner-driven shuffle). Aligning RFiles to splits is a future optimisation — a Spark-side adapter wrapping Accumulo's `KeyRangePartitioner` (or our own thin equivalent), driven from `repartitionAndSortWithinPartitions`. It is not a correctness requirement (FR-4 was relaxed accordingly).

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

**Risk**: Accumulo `RFile.append()` throws if keys are written out of order, so any disorder in the RDD partition handed to `AccumuloFileOutputFormat` aborts the task.

**Mitigation**: per target table, the job applies `sortByKey()` on the natural Accumulo `Key` ordering immediately before `saveAsNewAPIHadoopFile`. The `(Key, Value)` pair RDD already uses `Key`'s `WritableComparable` ordering. Out-of-order failure would surface immediately at write time, not silently corrupt the output.

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

**Risk**: with wrong split points, target tablets are unbalanced; produced RFiles (which are not split-aligned — see §5.2) will trigger more re-splits at bulk-import time.

**Mitigation**: split points are calculated by the `DatasetGenerator` based on the actual distribution of the generated data (e.g., medians on key fields). Setup installs them on the target tables before import. The PoC tolerates non-aligned RFiles by design — bulk import handles the re-split — so this risk degrades performance, not correctness.

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
│   ├── transform/  MigrationJob.java, EventTransformer.java
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
- `accumulo-hadoop-mapreduce` 2.1.2 (contains `AccumuloFileOutputFormat` under `org.apache.accumulo.hadoop.mapreduce..`)
- `accumulo-minicluster` 2.1.2
- `hadoop-client` (version compatible with Accumulo 2.1.2; 3.3.x)
- `spark-core` 3.5.x with appropriate scope (provides `saveAsNewAPIHadoopFile` / Hadoop OutputFormat compatibility)
- `jackson-databind` for JSON
- `slf4j-api` + `logback-classic`
- `junit-jupiter` for tests
