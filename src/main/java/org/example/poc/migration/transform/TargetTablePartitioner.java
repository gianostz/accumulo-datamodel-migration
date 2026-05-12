package org.example.poc.migration.transform;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partitioner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Spark {@link Partitioner} aligned to a target table's split points (architecture §5.2).
 *
 * <p>Number of partitions = {@code splits.size() + 1}. For an Accumulo {@link Key} with row
 * {@code r}, the partition index follows Accumulo's split semantics — a split point {@code S}
 * is the <i>inclusive upper bound</i> of its tablet:
 * <ul>
 *   <li>{@code r <= splits[0]} → partition 0</li>
 *   <li>{@code splits[i-1] < r <= splits[i]} → partition {@code i}</li>
 *   <li>{@code r > splits[last]} → partition {@code splits.size()}</li>
 * </ul>
 *
 * <p>Without this alignment each produced RFile may cross tablet boundaries of the target table
 * and force Accumulo to split RFiles on the fly during bulk import — exactly the cost bulk import
 * is meant to avoid (FR-4).
 *
 * <p>Client-bypass note: this class touches only {@link Key} (data package), {@link Text}, and
 * Spark — no Accumulo client. UT-6 verifies this.
 */
public final class TargetTablePartitioner extends Partitioner {

    private static final long serialVersionUID = 1L;

    private final List<Text> splits;

    /**
     * @param splits target table split points; copied and sorted defensively. May be empty
     *               (single partition).
     */
    public TargetTablePartitioner(List<Text> splits) {
        Objects.requireNonNull(splits, "splits");
        List<Text> copy = new ArrayList<>(splits.size());
        for (Text s : splits) {
            copy.add(new Text(Objects.requireNonNull(s, "split element")));
        }
        Collections.sort(copy);
        this.splits = Collections.unmodifiableList(copy);
    }

    @Override
    public int numPartitions() {
        return splits.size() + 1;
    }

    @Override
    public int getPartition(Object o) {
        Text row;
        if (o instanceof Key k) {
            row = k.getRow();
        } else if (o instanceof Text t) {
            row = t;
        } else if (o instanceof String s) {
            row = new Text(s);
        } else {
            throw new IllegalArgumentException(
                    "TargetTablePartitioner expects a Key/Text/String, got " + o.getClass().getName());
        }
        // Collections.binarySearch returns the match index when found, or
        // -(insertion_point) - 1 when not. Insertion point = number of splits < row,
        // which is exactly the partition index for row > splits[ip-1] (and < splits[ip] if any).
        int idx = Collections.binarySearch(splits, row);
        return idx >= 0 ? idx : -(idx + 1);
    }
}
