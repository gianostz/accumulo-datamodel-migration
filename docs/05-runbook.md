# PoC Accumulo RFile Migration — Runbook

## 1. Purpose

Practical guide for running the PoC end-to-end. Intended both for those developing the code (Claude Code or developer) and for those performing the first manual verification runs.

## 2. Prerequisites

### 2.1 Required Software

| Component               | Version     | Notes                                                          |
|-------------------------|-------------|----------------------------------------------------------------|
| JDK                     | 17          | Required. `pom.xml` targets release 17; JDK 11 will not build. |
| Maven                   | 3.8+        | For build                                                      |
| Apache Accumulo libs    | 2.1.2       | Included via Maven dependency, no installation                 |
| Spark libs              | 3.5.x       | Included via Maven dependency                                  |
| Hadoop libs             | 3.3.x       | Included transitively                                          |
| Operating system        | Linux/macOS | Windows possible but discouraged for MiniAccumulo              |

### 2.2 Hardware Resources (for PoC Execution)

- 8 GB RAM available to the Java process.
- 5 GB free disk space in `/tmp` (for MiniAccumulo + staging RFiles).
- At least 4 cores (Spark `local[*]` uses all available cores).

### 2.3 Environment Variables

`JAVA_HOME` must point at a JDK 17 install. Verify with `java -version` (should print `17.x`) before running Maven — many Linux distros ship JDK 11 as the default, in which case the build fails with `invalid target release: 17`.

```bash
export JAVA_HOME=/path/to/jdk-17           # e.g. /usr/lib/jvm/java-17-openjdk-amd64
export JAVA_OPTS="-Xmx6g -XX:+UseG1GC"
```

## 3. Build

From the project root:

```bash
mvn clean package -DskipTests=false
```

The build automatically runs unit tests (UT-1 ... UT-6). If any fails, the build fails. Produces a fat-jar at `target/accumulo-rfile-migration-<version>-shaded.jar` (the `-shaded` suffix comes from the shade plugin's classifier).

To skip unit tests (not recommended in CI):

```bash
mvn clean package -DskipTests=true
```

## 4. Execution

### 4.1 Full Execution (Wrapper Script)

```bash
./scripts/run-poc.sh
```

The script performs:

1. Cleanup of staging directories and reports from previous runs.
2. Launch of `PoCMain` with default configuration (`application.conf` on classpath).
3. Log tail on stdout.
4. Print of a final summary (global PASS/FAIL, report paths).

### 4.2 Manual Execution

```bash
java $JAVA_OPTS \
  -jar target/accumulo-rfile-migration-1.0.0-SNAPSHOT-shaded.jar
```

`application.conf` ships inside the shaded jar (`src/main/resources/application.conf`). To override it with an external file, prepend it to the classpath and run the main class directly:

```bash
java $JAVA_OPTS \
  -cp conf:target/accumulo-rfile-migration-1.0.0-SNAPSHOT-shaded.jar \
  org.example.poc.migration.orchestrate.PoCMain
```

### 4.3 Configuration Override

All parameters in `application.conf` can be overridden via system properties (Typesafe Config resolves them on `ConfigFactory.load()`):

```bash
java $JAVA_OPTS \
  -Ddataset.totalEvents=50000 \
  -Ddataset.randomSeed=123 \
  -Dwaves.count=2 \
  -jar target/accumulo-rfile-migration-1.0.0-SNAPSHOT-shaded.jar
```

## 5. Execution Phases and What to Expect in the Logs

### Phase 1 — MiniAccumulo Startup (~30–60 seconds)

```
[INFO] Starting MiniAccumuloCluster in /tmp/mini-accumulo-poc...
[INFO] MiniAccumulo started: instance=miniInstance, zk=localhost:NNNN
[INFO] Created tables: events_legacy, events_by_id, events_by_user, events_by_session, event_components_searchable, event_stats_by_type
[INFO] Applied split points to target tables.
```

### Phase 2 — Dataset Generation (~5–30 seconds)

```
[INFO] Generating 10000 events (seed=42)...
[INFO] Ingested 10000 events into events_legacy.
[INFO] Forcing major compaction on events_legacy...
[INFO] Compaction complete. RFile count: 1
```

### Phase 3 — Wave 1 (~1–3 minutes)

```
[INFO] === WAVE 1/2 START ===
[INFO] Wave range: user-000 to user-049
[INFO] Located 1 source RFile(s) covering wave range.
[INFO] Spark job: transforming RFile(s) -> 5 target tables...
[INFO] Spark job complete. Produced: events_by_id=N, events_by_user=N, ..., event_components_searchable=3N, event_stats_by_type=N
[INFO] Bulk importing into events_by_id... DONE (1.2s)
[INFO] Bulk importing into events_by_user... DONE (1.1s)
[INFO] Bulk importing into events_by_session... DONE (1.2s)
[INFO] Bulk importing into event_components_searchable... DONE (1.8s)
[INFO] Bulk importing into event_stats_by_type... DONE (1.1s)
[INFO] Running consistency verification...
[INFO]   CC-1 (counts)            : PASS
[INFO]   CC-2 (round-trip)        : PASS (sample=250, mismatches=0)
[INFO]   CC-3 (referential)       : PASS (orphans=0)
[INFO]   CC-4 (checksum)          : PASS
[INFO] Deleting source range user-000...user-049 from events_legacy...
[INFO] Forcing compaction on events_legacy...
[INFO] events_legacy: 12.5 MB -> 6.3 MB
[INFO] Wave 1 report: /tmp/poc-reports/wave-1-20260511-100432.json
[INFO] === WAVE 1/2 END (PASS) ===
```

### Phase 4 — Wave 2 (~1–3 minutes)

Analogous to the previous, on range `user-050` ... `user-099`.

### Phase 5 — Final Verification and Shutdown

```
[INFO] Final verification: events_legacy is empty (0 rows).
[INFO] Final verification: total entries in target tables match expected.
[INFO] === POC COMPLETE: PASS ===
[INFO] Reports: /tmp/poc-reports/
[INFO] Shutting down MiniAccumuloCluster...
```

## 6. Interpreting Results

### 6.1 Global Outcome: PASS

All acceptance criteria (AC-1 ... AC-7 from the Requirements Document) are satisfied. The PoC is considered passed.

Wave reports in `/tmp/poc-reports/` contain the detail of metrics and verifications.

### 6.2 Global Outcome: FAIL

The log indicates the failing verification. Common cases:

| Symptom                                    | Probable Cause                                           | Action                                                                |
|--------------------------------------------|----------------------------------------------------------|-----------------------------------------------------------------------|
| CC-1 counts differ from expected values    | Transformation missing entries for some event            | Inspect Spark job, verify transformation schema                       |
| CC-2 round-trip mismatch                  | Asymmetric serialization/deserialization                 | Check `EventSerializer`; verify JSON key ordering                     |
| CC-3 orphan eventIds                       | Partial bulk import failure / misaligned keys            | Check bulk import logs; re-run single wave                            |
| CC-4 different checksums                   | Lost or duplicate events                                 | Combine with CC-1; check `SourceRFileLocator`                         |
| OutOfMemoryError in Spark job              | Dataset too large for allocated RAM                      | Increase `-Xmx`; reduce `dataset.totalEvents` for the PoC            |
| Bulk import fails with "out of range"      | Target table split points not aligned to data            | Recalculate splits on the actual dataset (see generator logs)         |

## 7. Post-Execution Cleanup

```bash
./scripts/cleanup.sh
```

Removes:
- `/tmp/mini-accumulo-poc/`
- `/tmp/poc-staging/`
- `/tmp/poc-reports/` (optional — asks for confirmation)

## 8. Manual Inspection (Debug)

### 8.1 Inspecting a Produced RFile

```bash
java -cp target/accumulo-rfile-migration-1.0.0-SNAPSHOT-shaded.jar \
  org.apache.accumulo.core.file.rfile.PrintInfo \
  --dump /tmp/poc-staging/events_by_id/part-r-00000.rf
```

`scripts/inspect-results.sh` wraps this for all `*.rf` files in the staging dir.

### 8.2 Scanning a Target Table via Accumulo Shell

During execution, MiniAccumulo logs the access credentials (ZK port and password). An interactive shell can be opened:

```bash
accumulo shell -u root -p secret -zh localhost:NNNN
> scan -t events_by_id -np
```

### 8.3 Checking Disk Space

```bash
du -sh /tmp/mini-accumulo-poc/accumulo/tables/*/
```

Useful for validating the space reduction after deletion (Phase 3, last log lines).

## 9. Extensions and Parameters to Explore

Once the base PoC is validated, useful experiments:

- **Increase the dataset**: set `dataset.totalEvents` to 100k, 1M, to measure single-node scalability.
- **Vary the number of waves**: 4, 8 waves to measure setup overhead vs pure throughput.
- **Add split-aligned partitioning**: the current write path lets `importDirectory()` re-split RFiles on the fly (see architecture §5.2). Adding a Spark partitioner aligned to target splits should reduce bulk-import time — measure how much.
- **Inject a simulated failure** in one of the imports to test IT-5.

## 10. Final Notes

- The PoC is designed to be reproducible: the same seed produces the same dataset, and the transformation is deterministic. Two consecutive runs with the same configuration must produce substantially identical reports (modulo timing).
- Reports are intended to be archived and compared over time (e.g., after code changes).
- In case of a bug, **do not** delete the staging directories between a failure and a retry: they contain the `part-*.rf` files that can be inspected with `PrintInfo`.
