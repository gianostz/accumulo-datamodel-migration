# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository state

This repository currently contains **design documentation only** — no source code, build files, or scripts have been written yet. The PoC described in `docs/` has not been implemented. When asked to start implementing, scaffold the Maven project laid out in `docs/03-architecture.md` §7.

## Documentation map

The five docs in `docs/` are the source of truth and must be respected by any implementation:

| Doc | Authoritative for |
|-----|-------------------|
| `docs/01-requirements.md` | Functional/non-functional requirements (FR-1…FR-8, NFR-1…NFR-7), acceptance criteria (AC-1…AC-7). Out-of-scope list. |
| `docs/02-data-model.md` | Event JSON schema, the legacy `events_legacy` table layout, and the **exact key construction** for the 5 target tables. The transformation rule table in §5 is the implementation contract. |
| `docs/03-architecture.md` | Component decomposition (Setup/Transform/Import/Verify), Spark-vs-MR rationale, module boundary that enforces the client-bypass rule, project structure (§7), configuration shape (§8), dependency versions (§9). |
| `docs/04-test-plan.md` | UT-1…UT-6, IT-1…IT-5, the four consistency checks (CC-1…CC-4), and the wave-report JSON schema. |
| `docs/05-runbook.md` | How the PoC is expected to be built and run once implemented. |

If a code change conflicts with these docs, update the relevant doc in the same change rather than silently diverging.

## Load-bearing invariants

These are easy to violate by accident and break the whole PoC. Treat them as non-negotiable:

1. **No TabletServer traffic during transformation (NFR-4, AC-6).** Code that runs inside the Spark `flatMap`/`mapPartitions` must not import or use `BatchScanner`, `BatchWriter`, or `AccumuloClient`. Anything under `org.apache.accumulo.core.client..` / `clientImpl..` is also banned — with the precise carve-out below. Allowed: the static-file RFile read API, Hadoop `FileSystem`, and `org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat` (the Hadoop OutputFormat that wraps the static RFile writer — verified by disassembly to make no cluster call from `getRecordWriter()`). The project is a single Maven module, so this is enforced at the **package** level by an ArchUnit rule (UT-6) scoped to `org.example.poc.migration.transform..`. Do not weaken or scope-narrow that rule to make a test easier — failing UT-6 is the only thing standing between us and a silent NFR-4 violation.

   **Carve-outs:** the public RFile read API in Accumulo 2.1.x lives at `org.apache.accumulo.core.client.rfile.*` and `RFile.newScanner().build()` returns `org.apache.accumulo.core.client.Scanner`. UT-6 therefore exempts — and **only** exempts — everything under `org.apache.accumulo.core.client.rfile..`, plus the two types `org.apache.accumulo.core.client.Scanner` and `org.apache.accumulo.core.client.ScannerBase`. The `Scanner` reference is intentionally confined to `transform/RFileIO.java` (callers see an `Iterable<Map.Entry<Key,Value>>` view). `AccumuloFileOutputFormat` lives in `org.apache.accumulo.hadoop.mapreduce..`, outside the banned namespace, so it is permitted unconditionally. Widening the carve-out further into `accumulo.core.client..` requires updating this invariant and `ClientBypassArchTest`'s javadoc together; do not introduce a new exemption silently.
2. **Determinism / idempotency (NFR-3, §7 of data model).** Transformation output for a given source event must be byte-identical across runs. No `now()`, no random UUIDs, no map iteration order leaking into keys. Re-running a wave on the same input must overwrite the same keys with the same timestamps.
3. **Key ordering and split alignment.** Per-partition output to an RFile must be sorted by Accumulo's natural `Key` ordering (`RFile.newWriter().append()` throws otherwise). Spark partitioning must use the target table's split points (read via `listSplits` on the driver, broadcast to executors) so each produced RFile lies entirely within one tablet — otherwise bulk import does an expensive on-the-fly split.
4. **Per-source-event fan-out is 7 entries across 5 tables** (1+1+1+3+1). Counts in CC-1 depend on this exact ratio. See `docs/02-data-model.md` §5 for the canonical row-id construction (note specifically: `reverseTimestamp = MAX_LONG - ts` padded to 19 chars; `pad(n) = String.format("%019d", n)`).
5. **Preserve source timestamps** on produced KVs (architecture §5.4). This is what makes re-import idempotent and lets versioning distinguish migrated from freshly-ingested data.

## Decisions still open

`docs/README.md` flags these as needing confirmation before code is written. Ask the user before locking them in:

- Exact Spark 3.5.x version compatible with the Hadoop 3.3.x that Accumulo 2.1.2 pulls transitively.
- Implementation language: Java (default, recommended) vs Scala.
- Wave split strategy: `userIdMedian` (proposed) vs `byTabletRange`.
- Event serialization: JSON for the PoC; binary (Avro/Protobuf) deferred to production.

## Expected commands (once implemented)

The runbook prescribes these — do not invent alternatives without updating `docs/05-runbook.md`:

```bash
mvn clean package                  # builds fat-jar, runs UT-1..UT-6
./scripts/run-poc.sh               # full end-to-end (2 waves) with defaults
java $JAVA_OPTS -Dconfig.file=conf/application.conf \
  -jar target/poc-migration-<version>-shaded.jar
```

Config overrides go through `-D` system properties (e.g. `-Ddataset.totalEvents=50000`, `-Ddataset.randomSeed=123`). All tunables live in `application.conf` (HOCON); see architecture §8 for the canonical shape.

## Working directory layout

Per architecture §7, the project is a **single Maven module** (root `pom.xml`). Java sources live under `src/main/java/org/example/poc/migration/` and are organized by phase: `env/`, `data/`, `locate/`, `transform/`, `ingest/`, `verify/`, `clean/`, `orchestrate/`, `util/`. The package boundary around `transform/` is what enforces NFR-4 — never put Accumulo client code there, and never relax the ArchUnit rule that polices it.
