# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

`docs/DESIGN.md` is the full design reference (scope, model, fan-out rules, flow, non-goals). Keep it in sync with code changes.

## Goal

Demonstrate a minimal read-transform-write pipeline for Accumulo RFiles:

1. Write synthetic `Event` records into a source Accumulo table (via `BatchWriter`).
2. Compact to RFiles, then locate them via the `accumulo.metadata` table.
3. Read source RFiles directly using `RFile.newScanner()` — no TabletServer traffic.
4. Transform each entry into 3 target-table entries (hardcoded fan-out).
5. Write sorted target entries to new RFiles via `RFile.newWriter()`.
6. Bulk-import the RFiles into 3 new Accumulo tables.
7. Scan each target table to verify.

## Tech stack

| Tool | Version |
|------|---------|
| Scala | 3.3.3 |
| SBT | 1.9.9 |
| Accumulo | 2.1.2 (MiniAccumuloCluster for the demo) |
| Hadoop | 3.3.4 (transitive, explicit for `hadoop-common`) |

## Running

```bash
sbt run          # starts mini-Accumulo, runs the full pipeline, prints results
```

Java 11+ required. If `JAVA_HOME` points to Java 11, SBT picks it up automatically.

## Source layout

```
src/main/scala/org/example/migration/
  Main.scala                # orchestrates all steps
  model/
    Event.scala             # case class + EventSerializer (change model here)
  setup/
    SourceWriter.scala      # BatchWriter + compact → RFiles
  read/
    RFileLocator.scala      # metadata table scan → RFile URIs
    RFileReader.scala       # RFile.newScanner() → Seq[(Key, Value)]
  transform/
    Transformer.scala       # 1 entry → 3 (tableName, Key, Value) tuples
  write/
    RFileWriter.scala       # RFile.newWriter() — sorts entries before writing
  ingest/
    BulkImporter.scala      # tableOperations().importDirectory().to().load()
  verify/
    TableVerifier.scala     # BatchScanner scan + stdout
```

## Changing the domain model

Edit `model/Event.scala`:
- Change the `Event` case class fields.
- Update `EventSerializer.toKey`, `toValue`, and `fromEntry` to match.
- Update `Transformer` key constructors if the fan-out schema also changes.

Nothing else needs touching for a field rename.

## Key invariants

- **No TabletServer traffic during the read step.** `RFileReader` uses `RFile.newScanner()` only; it must not import `BatchScanner` or `AccumuloClient`.
- **RFile entries must be sorted.** `RFileWriter.write()` sorts by `Key.compareTo()` before appending; do not remove that sort.
- **`fork := true` in build.sbt is load-bearing.** MiniAccumuloCluster needs a forked JVM to find its classpath correctly.
