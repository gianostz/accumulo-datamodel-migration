# PoC Accumulo RFile Migration — Documentation Index

This is the complete documentation for the Accumulo data migration PoC, which demonstrates the transformation of RFiles from a legacy data model to a new data model, **bypassing the Accumulo client** during the transformation phase and using bulk import only for the final registration step.

## Document Structure

| # | Document | Contents |
|---|----------|----------|
| 1 | [01-requirements.md](./01-requirements.md) | Functional and non-functional requirements, scope, acceptance criteria |
| 2 | [02-data-model.md](./02-data-model.md) | Event schema, legacy source table, 5 target tables, transformation rules |
| 3 | [03-architecture.md](./03-architecture.md) | Components, data flow, **Spark vs MapReduce** comparison with rationale, design choices, risks |
| 4 | [04-test-plan.md](./04-test-plan.md) | Unit tests, integration tests, consistency checks, report format |
| 5 | [05-runbook.md](./05-runbook.md) | Build, execution, result interpretation, troubleshooting |
| 6 | [06-implementation-plan.md](./06-implementation-plan.md) | Living phase-by-phase implementation plan; handoff doc for future Claude sessions |

## How to Use This Documentation

**For the implementer (Claude Code or developer)**: read in the order listed. The architecture document drives the code structure; the data model defines the precise transformations; the test plan defines the assertions to implement.

**For reviewers**: start with the requirements to validate scope, then move to the data model to verify semantic correctness, and finally inspect the technical decisions in the architecture document (especially section 4 on the Spark/MapReduce comparison).

**For those running the PoC**: go directly to the runbook.

## Key Points of the Proposed Solution

1. **Bypass of the Accumulo client**: the data transformation uses only `RFile.newReader()` / `RFile.newWriter()`, never `Scanner` or `BatchWriter`. The client is allowed only for setup, bulk import, verification, and cleanup.
2. **1→5 table fan-out**: from a single source table, 5 target tables with different schemas are produced, each optimized for the access patterns of the new model.
3. **Wave-based execution**: demonstration of 2 sequential waves with progressive deletion of the source data, to validate the space constraint.
4. **Consistency checks**: 4 dimensions (counts, round-trip, referential integrity, aggregate checksum) ensure that the migration neither loses nor alters data.
5. **Apache Spark in local mode**: the technology chosen for the PoC, with rationale documented in 03-architecture.md section 4.
6. **MiniAccumuloCluster**: everything runs in-process on a single machine, with no dependencies on external clusters.

## Decisions Requiring Confirmation or Further Investigation

Several points that should be confirmed or explored further before writing code:

- **Exact Spark version** compatible with Hadoop 3.3.x (transitive dependency of Accumulo 2.1.2): suggested 3.5.x — verify during the initial prototype phase.
- **Language**: Java or Scala. Proposed default: Java (consistent with Accumulo).
- **Wave split strategy**: proposed `userIdMedian` (lexicographic split on users). Alternative: split by tablet range of the source table.
- **Event serialization**: JSON for the PoC. In production, a binary format will be evaluated.
