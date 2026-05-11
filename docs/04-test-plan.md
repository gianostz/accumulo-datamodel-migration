# PoC Accumulo RFile Migration — Test Plan and Verification

## 1. Plan Objectives

Define the tests that demonstrate the PoC is working and the verifications that attest to the consistency of the migrated data. The plan is structured across 3 levels:

1. **Unit tests**: isolated components (transformation, partitioner, key construction).
2. **Integration tests**: end-to-end on MiniAccumuloCluster with a small dataset (~100 events).
3. **Full PoC execution**: standard dataset (~10,000 events), 2 waves, complete consistency checks.

## 2. Unit Tests

### UT-1 — EventTransformer: Correct Fan-Out
**Given** a source event with all fields populated  
**When** `EventTransformer.transform(keyValue)` is applied  
**Then** exactly 7 `(targetTable, Key, Value)` tuples are obtained, distributed as:
- 1 entry in `events_by_id`
- 1 entry in `events_by_user`
- 1 entry in `events_by_session`
- 3 entries in `event_components_searchable` (ip, resource, type)
- 1 entry in `event_stats_by_type`

### UT-2 — EventTransformer: Idempotency
**Given** a source event  
**When** the transformation is applied twice  
**Then** both outputs are byte-identical (same Keys, same Values, same timestamps).

### UT-3 — KeyUtils: reverseTimestamp is Lexicographically Ordering
**Given** two timestamps `t1 < t2`  
**When** `pad(MAX_LONG - t)` is computed for both  
**Then** the string for `t2` sorts lexicographically BEFORE the string for `t1` (descending temporal order).

### UT-4 — TargetTablePartitioner: Correct Distribution
**Given** a set of split points and a set of Keys falling within each interval  
**When** the partitioner is applied  
**Then** each Key falls in the correct partition (= index of the target tablet).

### UT-5 — RFile Round-Trip
**Given** a set of N ordered `(Key, Value)` pairs  
**When** an RFile is written and then read back  
**Then** the same N pairs are obtained in the same order.

### UT-6 — Static Verification of Accumulo Client Bypass
**Given** the classes in package `org.example.poc.migration.transform..`  
**When** their imports are inspected  
**Then** no references to `Scanner`, `BatchScanner`, `BatchWriter`, `AccumuloClient`, or any type under `org.apache.accumulo.core.client..` / `org.apache.accumulo.core.clientImpl..` are present.

**Implementation**: ArchUnit rule in `src/test/java/.../transform/ClientBypassArchTest.java`. The project is single-module (architecture §7), so this test is the binding enforcement of NFR-4 — it must fail the build if violated.

## 3. Integration Tests (Small Dataset, ~100 Events)

### IT-1 — Setup and Generation
**When** MiniAccumulo is started, tables are created, and the dataset is generated  
**Then** the `events_legacy` table contains exactly N entries (N = dataset size).

### IT-2 — Single Wave End-to-End
**When** a single wave is executed on the full dataset (one wave only, no split between waves)  
**Then**:
- The 5 target tables contain the expected counts (see below).
- `events_legacy` is empty after the final `deleteRows`.
- The wave report is generated and contains all metrics.

### IT-3 — Two Sequential Waves
**When** the full PoC is executed with 2 waves  
**Then**:
- At the end of the first wave, `events_legacy` contains only the second half (verifiable by range).
- At the end of the second wave, `events_legacy` is empty.
- Target tables contain the total, identical to the IT-2 case.

### IT-4 — Wave Idempotency
**When** the same wave is re-run on the same input after it has already been executed  
**Then**:
- The secondary bulk import does not create duplicates (Keys are identical; Accumulo keeps a single version per Key+ts).
- Counts in the target tables remain unchanged.
- The aggregate checksum remains unchanged.

### IT-5 — Recovery from Partial Bulk Import Failure (Manual or Simulated Test)
**Given** a wave in which a failure of `importDirectory` for the 4th table is simulated  
**When** the PoC detects the failure  
**Then**:
- The system aborts the wave (cleans up already successful imports).
- State is restored: target tables are back to their pre-wave state.
- A retry of the wave completes without errors.

(Optional test for the PoC; may be documented as a "manual verification" if failure injection is complex.)

## 4. Consistency Checks (Executed by `ConsistencyVerifier`)

### CC-1 — Counts per Target Table

Calculate expected counts from the generated source dataset:

| Table                         | Expected Count               |
|-------------------------------|------------------------------|
| events_by_id                  | N (= number of source events)|
| events_by_user                | N                            |
| events_by_session             | N                            |
| event_components_searchable   | 3 × N                        |
| event_stats_by_type           | N                            |

Perform a full scan on each target table and count the rows. Compare with expected values. **Pass**: all counts match.

### CC-2 — Semantic Round-Trip

1. Select a random sample of K eventIds (K = max(50, 5% of N), seeded for reproducibility).
2. For each eventId in the sample:
   - Retrieve the event from `events_by_id` (direct lookup).
   - Compare byte-for-byte with the original source event (kept in memory or recalculated by `DatasetGenerator` with the same seed).
3. **Pass**: 0 mismatches.

### CC-3 — Referential Integrity

For each table `events_by_user`, `events_by_session`, `event_components_searchable`, `event_stats_by_type`:

1. Full scan, extracting the `value` (which contains an `eventId`).
2. For each collected eventId, verify via a lookup in `events_by_id` that it exists.
3. **Pass**: 0 orphan eventIds.

Optimization: instead of N individual lookups, build an in-memory set of eventIds present in `events_by_id` (for PoC datasets up to 100,000 events this is perfectly feasible) and compare via set difference.

### CC-4 — Aggregate Checksum

1. Extract from the source dataset the list of `eventId`s (sorted lexicographically).
2. Extract from `events_by_id` the list of `eventId`s (sorted lexicographically).
3. Compute SHA-256 on the concatenation (separator: `\n`) of both lists.
4. **Pass**: the two hashes match.

## 5. Collected and Reported Metrics

The `WaveOrchestrator` collects and includes in the wave report the following metrics:

### Volume Metrics
- Number of source RFiles read.
- Total size of source RFiles (bytes).
- Number of KeyValues read.
- Number of KeyValues produced per target table.
- Number of RFiles produced per target table.
- Total size of RFiles produced per target table (bytes).

### Timing Metrics (per phase)
- Preventive compaction of source table (ms).
- RFile location (ms).
- Spark transformation job (ms), wall-clock and CPU.
- Bulk import per target table (ms each).
- Consistency verification (ms).
- `deleteRows` + compaction (ms).
- **Total wave** (ms).

### Verification Outcomes
- CC-1: PASS / FAIL + count details.
- CC-2: PASS / FAIL + number of mismatches.
- CC-3: PASS / FAIL + number of orphans.
- CC-4: PASS / FAIL + hashes compared.

### Disk Footprint
- Size of `events_legacy` directory before and after `deleteRows + compaction`.
- Size of each target table directory before and after import.

## 6. Report Format

The wave report is a JSON file saved to `${paths.reportsDir}/wave-<N>-<timestamp>.json`:

```json
{
  "waveNumber": 1,
  "startTime": "2026-05-11T10:00:00Z",
  "endTime":   "2026-05-11T10:04:32Z",
  "totalDurationMs": 272000,
  "source": {
    "rfileCount": 3,
    "rfileTotalBytes": 12500000,
    "keyValueCount": 5000
  },
  "transformation": {
    "durationMs": 45000,
    "produced": {
      "events_by_id":                { "keyValueCount": 5000,  "rfileCount": 3, "totalBytes": 2100000 },
      "events_by_user":              { "keyValueCount": 5000,  "rfileCount": 3, "totalBytes": 1800000 },
      "events_by_session":           { "keyValueCount": 5000,  "rfileCount": 3, "totalBytes": 1900000 },
      "event_components_searchable": { "keyValueCount": 15000, "rfileCount": 3, "totalBytes": 4500000 },
      "event_stats_by_type":         { "keyValueCount": 5000,  "rfileCount": 3, "totalBytes": 1700000 }
    }
  },
  "bulkImport": {
    "events_by_id":                { "durationMs": 1200 },
    "events_by_user":              { "durationMs": 1100 },
    "events_by_session":           { "durationMs": 1150 },
    "event_components_searchable": { "durationMs": 1800 },
    "event_stats_by_type":         { "durationMs": 1050 }
  },
  "verification": {
    "CC-1_counts":              { "outcome": "PASS", "details": {...} },
    "CC-2_roundTrip":           { "outcome": "PASS", "sampleSize": 250, "mismatches": 0 },
    "CC-3_referentialIntegrity":{ "outcome": "PASS", "orphans": 0 },
    "CC-4_checksum":            { "outcome": "PASS", "sourceHash": "...", "targetHash": "..." }
  },
  "cleanup": {
    "deletedRowRange": { "start": "user-000_...", "end": "user-049_..." },
    "preDeleteBytes":  12500000,
    "postCompactionBytes": 6300000
  },
  "overallOutcome": "PASS"
}
```

## 7. Test Plan Acceptance Criteria

The plan is considered **passed** if:

1. All unit tests (UT-1 ... UT-6) are PASS.
2. Integration tests IT-1, IT-2, IT-3, IT-4 are PASS.
3. The full PoC execution on the standard dataset produces 2 reports (one per wave) with `overallOutcome: PASS`.
4. All acceptance criteria from the Requirements Document (AC-1 ... AC-7) are satisfied.

IT-5 is optional but recommended.
