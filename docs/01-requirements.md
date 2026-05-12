# PoC Accumulo RFile Migration — Requirements Document

## 1. Context

The production system uses Apache Accumulo 2.1.2 as its primary storage. Following new functional requirements, the data model has been updated: current ingestion follows the new model, while the historical data (~2TB) remains under the old model.

Re-ingesting historical data through the Accumulo client (Scanner + BatchWriter) would have two significant drawbacks:

1. **High operational load on the production cluster**: every write goes through the TabletServer, write-ahead log, and subsequent compactions.
2. **Extended execution time**: the write → flush → compaction pipeline is significantly slower than directly writing pre-sorted RFiles.

The identified strategy consists of **transforming RFiles outside of the TabletServer write path** (reading via `RFile.newReader()`, writing via Accumulo's `AccumuloFileOutputFormat` from `accumulo-hadoop-mapreduce`) and subsequently registering them into the target tables via **bulk import** (`TableOperations.importDirectory()`). `AccumuloFileOutputFormat` is a thin Hadoop `OutputFormat` wrapper that ultimately delegates to the same static-file RFile writer, but is battle-tested for block sizing, compression, and version handling.

## 2. PoC Objective

Demonstrate, at reduced scale and in a controlled environment (MiniAccumuloCluster), that transforming Accumulo data from one model to another can be achieved:

- **Externally to the Accumulo client** for the data transformation phase (direct RFile read/write).
- **With 1→N fan-out**: from a single source table, N target tables with different schemas are produced.
- **Consistently**: data in the target tables is semantically equivalent to the source data and verifiable.
- **In batches (waves)**: with progressive deletion of migrated source data, to validate the management of space constraints.

## 3. Scope

### 3.1 In Scope

- Generation of a test dataset (~10,000–100,000 synthetic events) in the source table `events_legacy`.
- Implementation of a transformation job that produces RFiles for 5 target tables.
- Bulk import of generated RFiles into the target tables.
- Consistency verification between source and destination.
- Execution of **2 sequential waves**, with deletion of migrated source data between waves.
- Documentation of results and metrics (timing, rows processed, output size).

### 3.2 Out of Scope

- Scalability: the PoC does not measure performance at real data volumes. Scalability validation is the subject of a subsequent phase.
- Handling concurrent writes to the source table during migration.
- Automatic rollback strategies in case of partial failure (to be covered in production operational documentation).
- Fine-tuning of Accumulo parameters (cache, compression, block size).
- Fault tolerance of the transformation job (distributed retry, checkpointing).

## 4. Functional Requirements

### FR-1 — Source Dataset Generation
The system must be able to generate a dataset of N synthetic events (configurable) and insert them into the `events_legacy` table according to the legacy model described in the Data Model Document.

### FR-2 — Source RFile Extraction
The system must be able to identify the physical RFiles associated with the source table (by reading the `accumulo.metadata` table or forcing a compaction to obtain a known set of files).

### FR-3 — External Transformation
The system must read source RFiles using `RFile.newReader()` (without routing data through Accumulo TabletServers during transformation), deserialize the events, and produce 5 sets of RFiles (one per target table) by emitting `(Key, Value)` pairs through `AccumuloFileOutputFormat` (which itself delegates to the static-file RFile writer).

### FR-4 — Compliance of Produced RFiles
The produced RFiles must comply with Accumulo format constraints, in particular:
- Keys sorted according to Accumulo ordering (rowId, cf, cq, vs, ts descending).

Alignment of produced RFiles to target-table split points is **not** required: `TableOperations.importDirectory()` accepts RFiles that span tablet boundaries and will internally re-split them at import time. The PoC accepts the (small) bulk-import cost of on-the-fly re-splits in exchange for a substantially simpler Spark write path. Aligning RFiles to splits is a future optimisation, not a correctness requirement.

### FR-5 — Bulk Import
The system must register the produced RFiles into the target tables via `TableOperations.importDirectory()`.

### FR-6 — Consistency Verification
After bulk import, the system must perform the following verifications:
- **Counts**: for each source event, exactly 1 entry in `events_by_id`, `events_by_user`, `events_by_session`, `event_stats_by_type`, and N entries in `event_components_searchable` (where N = number of searchable fields per event).
- **Semantic round-trip**: a sample of eventIds reconstructed from `events_by_id` must be byte-equivalent to the original source event.
- **Referential integrity**: every eventId referenced in the "by_user", "by_session", "components", and "stats" tables must exist in `events_by_id`.
- **Aggregate checksum**: hash of the sorted concatenation of source eventIds vs hash of eventIds in `events_by_id` (must match).

### FR-7 — Wave-Based Execution
The PoC must be executable in **2 sequential waves**:
- Wave 1: transforms half the source dataset, imports into targets, verifies, deletes the migrated source data.
- Wave 2: transforms the remaining half, imports, verifies, deletes.
At the end, the source table must be empty (or deletable) and the target tables must contain the complete dataset.

### FR-8 — Reporting
After each wave, the system must produce a report (text or JSON file) containing: number of events processed, counts per target table, verification outcomes, and timing per phase.

## 5. Non-Functional Requirements

### NFR-1 — Execution Environment
The PoC must be executable on a single developer machine, using MiniAccumuloCluster (Accumulo 2.1.2). No dependency on an external Hadoop/Accumulo cluster.

### NFR-2 — Reproducibility
The PoC execution must be fully automated via script (single entry-point command) and must produce deterministic results given the same dataset generation seed.

### NFR-3 — Transformation Idempotency
Transforming a source event must produce deterministic keys in the target tables. Re-running the transformation on the same input must produce byte-identical output.

### NFR-4 — No TabletServer Traffic During Transformation
The transformation must not route data through Accumulo TabletServers. `Scanner`, `BatchScanner`, and `BatchWriter` are forbidden during transformation. Read-only metadata access (e.g., split point queries) is permitted. The Accumulo client is permitted for setup, split reads, bulk import, and verification.

Concretely, the static-file RFile API (`RFile.newReader()` / `RFile.newWriter()`) and the Hadoop OutputFormat that wraps it (`org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat`) are both allowed — neither opens a TabletServer connection. The narrow read-side carve-out for `org.apache.accumulo.core.client.Scanner` (the return type of `RFile.newScanner().build()`) remains, since that `Scanner` is bound to an RFile path, not to a cluster.

### NFR-5 — Technology
The transformation job must be implemented using **Apache Spark in local[*] mode**. Motivation and comparison with alternatives (MapReduce) are in the Architecture Document.

### NFR-6 — Language
The code must be written in **Java** or **Scala** (at the implementer's discretion). Java is preferred for consistency with the Accumulo ecosystem; Scala for Spark code conciseness.

### NFR-7 — Build System
Maven or Gradle, with all dependencies declared and versions pinned. Reproducible build.

## 6. Acceptance Criteria

The PoC is considered **passed** if, at the end of the complete execution (2 waves):

| ID   | Criterion                                                                                               | Measure                             |
|------|---------------------------------------------------------------------------------------------------------|-------------------------------------|
| AC-1 | The source table is empty                                                                               | Scan = 0 rows                       |
| AC-2 | All 5 target tables contain the expected counts                                                         | Counts = calculated expected values |
| AC-3 | Semantic round-trip successful for 100% of the verification sample (sample ≥ 5% of dataset)            | 0 mismatches                        |
| AC-4 | Referential integrity at 100%                                                                           | 0 orphan eventIds                   |
| AC-5 | Source aggregate checksum = `events_by_id` aggregate checksum                                          | Identical hashes                    |
| AC-6 | The transformation code does not use `BatchScanner`, `BatchWriter`, or `AccumuloClient` during transformation. Read-only metadata access via `AccumuloFileOutputFormat` is permitted. | Static code verification            |
| AC-7 | Wave reports are generated and contain all required metrics                                             | Presence of report files            |

## 7. Constraints and Assumptions

- **Accumulo version**: 2.1.2 (matching production).
- **Spark version**: 3.x compatible with the Hadoop version used by Accumulo 2.1.2.
- **Event schema**: defined in the Data Model Document. The PoC assumes a stable, known schema at the time of transformation.
- **No custom RFile encryption/compression**: default Accumulo settings are used.
- **Single-node**: the PoC does not validate cross-node partitioning behavior; this must be validated in a subsequent scalability test.

## 8. Deliverables

1. Source code for the transformation job and setup/verification scripts.
2. End-to-end execution script (single command).
3. Reports generated during execution (FR-8).
4. Operational README (see Runbook document).
