package org.example.poc.migration.transform;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * One row of the {@code EventTransformer} fan-out (data-model §5): the target table name plus
 * the Accumulo {@link Key}/{@link Value} pair to be written to it.
 *
 * Lives in {@code transform/} on purpose — it is touched by Spark executor-side code and must
 * stay client-free. {@link Key} and {@link Value} are in {@code org.apache.accumulo.core.data},
 * not {@code .client}, so they are allowed under the UT-6 ArchUnit rule.
 */
public record TargetEntry(String targetTable, Key key, Value value) {
}
