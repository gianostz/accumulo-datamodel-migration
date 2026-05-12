package org.example.poc.migration.transform;

import java.util.List;
import java.util.Map;

/**
 * Outcome of a {@link MigrationJob} run. The two maps are keyed by target table name.
 *
 * @param stagingPathsByTable absolute (URI-form) paths of the per-partition RFiles produced
 *                            for each target table. Empty partitions are skipped, so the size
 *                            of the list is between 0 and {@code splits.size() + 1}.
 * @param entryCountsByTable  the number of {@code KeyValue} entries actually written for
 *                            each target table. CC-1 (test-plan §4.1) compares these against
 *                            the expected {@code N} or {@code 3N}.
 */
public record TransformResult(
        Map<String, List<String>> stagingPathsByTable,
        Map<String, Long> entryCountsByTable) {
}
