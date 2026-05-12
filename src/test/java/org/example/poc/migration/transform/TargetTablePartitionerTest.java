package org.example.poc.migration.transform;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * UT-4 — {@link TargetTablePartitioner} maps each Key to the partition index of the tablet
 * that would receive it under Accumulo split semantics (architecture §5.2).
 *
 * <p>Accumulo split point {@code S} is the <i>inclusive upper bound</i> of its tablet, so
 * a key whose row equals {@code S} belongs to the tablet ending at {@code S}, not the next one.
 */
class TargetTablePartitionerTest {

    private static Key keyForRow(String row) {
        return new Key(new Text(row));
    }

    @Test
    void emptySplits_allKeysLandInPartitionZero() {
        TargetTablePartitioner p = new TargetTablePartitioner(List.of());
        assertEquals(1, p.numPartitions());
        assertEquals(0, p.getPartition(keyForRow("anything")));
        assertEquals(0, p.getPartition(keyForRow("")));
        assertEquals(0, p.getPartition(keyForRow("￿")));
    }

    @Test
    void numPartitions_isSplitsSizePlusOne() {
        TargetTablePartitioner p = new TargetTablePartitioner(
                List.of(new Text("a"), new Text("m"), new Text("z")));
        assertEquals(4, p.numPartitions());
    }

    @Test
    void rowBelowFirstSplit_landsInPartitionZero() {
        TargetTablePartitioner p = new TargetTablePartitioner(
                List.of(new Text("m"), new Text("s")));
        assertEquals(0, p.getPartition(keyForRow("a")));
        assertEquals(0, p.getPartition(keyForRow("l")));
    }

    @Test
    void rowEqualToSplit_landsInThatSplitsPartition_inclusiveUpperBound() {
        // splits ["m", "s"] → tablets (-inf, m], (m, s], (s, +inf)
        // row "m" belongs to partition 0 (tablet ending at m, inclusive).
        // row "s" belongs to partition 1 (tablet ending at s, inclusive).
        TargetTablePartitioner p = new TargetTablePartitioner(
                List.of(new Text("m"), new Text("s")));
        assertEquals(0, p.getPartition(keyForRow("m")));
        assertEquals(1, p.getPartition(keyForRow("s")));
    }

    @Test
    void rowBetweenSplits_landsInMiddlePartition() {
        TargetTablePartitioner p = new TargetTablePartitioner(
                List.of(new Text("m"), new Text("s")));
        assertEquals(1, p.getPartition(keyForRow("n")));
        assertEquals(1, p.getPartition(keyForRow("r")));
    }

    @Test
    void rowAboveLastSplit_landsInLastPartition() {
        TargetTablePartitioner p = new TargetTablePartitioner(
                List.of(new Text("m"), new Text("s")));
        assertEquals(2, p.getPartition(keyForRow("t")));
        assertEquals(2, p.getPartition(keyForRow("zzzz")));
    }

    @Test
    void unsortedSplits_areSortedDefensively() {
        TargetTablePartitioner p = new TargetTablePartitioner(
                List.of(new Text("s"), new Text("a"), new Text("m")));
        // After sorting: [a, m, s] → tablets (-inf, a], (a, m], (m, s], (s, +inf).
        assertEquals(0, p.getPartition(keyForRow("a")));
        assertEquals(1, p.getPartition(keyForRow("b")));
        assertEquals(1, p.getPartition(keyForRow("m")));
        assertEquals(2, p.getPartition(keyForRow("n")));
        assertEquals(2, p.getPartition(keyForRow("s")));
        assertEquals(3, p.getPartition(keyForRow("t")));
    }

    @Test
    void accepts_Text_and_String_as_well_as_Key() {
        TargetTablePartitioner p = new TargetTablePartitioner(List.of(new Text("m")));
        assertEquals(0, p.getPartition(new Text("a")));
        assertEquals(1, p.getPartition(new Text("n")));
        assertEquals(0, p.getPartition("a"));
        assertEquals(1, p.getPartition("n"));
    }

    @Test
    void unknownInputType_isRejected() {
        TargetTablePartitioner p = new TargetTablePartitioner(List.of(new Text("m")));
        assertThrows(IllegalArgumentException.class, () -> p.getPartition(42));
    }

    @Test
    void accepts_SerializableKey_thePathSparkActuallyUses() {
        // MigrationJob's shuffle key is SerializableKey — this is the production code path.
        TargetTablePartitioner p = new TargetTablePartitioner(List.of(new Text("m")));
        assertEquals(0, p.getPartition(new SerializableKey(new Key(new Text("a")))));
        assertEquals(1, p.getPartition(new SerializableKey(new Key(new Text("n")))));
        assertEquals(0, p.getPartition(new SerializableKey(new Key(new Text("m")))));
    }

    @Test
    void javaSerializationRoundTrip_preservesPartitionAssignments() throws Exception {
        // Spark sends the partitioner from the driver to executors via Java serialization.
        // Text is not Serializable, so this only works because of the writeObject/readObject
        // hooks on TargetTablePartitioner — assert that path is intact.
        TargetTablePartitioner original = new TargetTablePartitioner(
                List.of(new Text("user-100"), new Text("user-200"), new Text("user-300")));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(original);
        }
        TargetTablePartitioner roundTripped;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            roundTripped = (TargetTablePartitioner) in.readObject();
        }

        assertEquals(original.numPartitions(), roundTripped.numPartitions());
        for (String row : List.of("user-042", "user-100", "user-150", "user-200",
                                   "user-250", "user-300", "user-999")) {
            assertEquals(original.getPartition(keyForRow(row)),
                    roundTripped.getPartition(keyForRow(row)),
                    "partition assignment must survive Java serialization for row=" + row);
        }
    }

    @Test
    void realistic_userIdMedian_splitsForUsersTable() {
        // Mirrors data-model §8 example for events_by_user: split on userId boundaries.
        TargetTablePartitioner p = new TargetTablePartitioner(
                List.of(new Text("user-100"), new Text("user-200"), new Text("user-300")));
        assertEquals(4, p.numPartitions());
        assertEquals(0, p.getPartition(keyForRow("user-042_xxx")));
        assertEquals(1, p.getPartition(keyForRow("user-150_xxx")));
        assertEquals(2, p.getPartition(keyForRow("user-250_xxx")));
        assertEquals(3, p.getPartition(keyForRow("user-999_xxx")));
    }
}
