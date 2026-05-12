package org.example.poc.migration.transform;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * Spark-shippable wrapper around an Accumulo {@link Key}. {@code Key} implements
 * {@code WritableComparable} but <i>not</i> {@link java.io.Serializable}, so it cannot be
 * shuffled by Spark's default Java serializer without a wrapper.
 *
 * <p>{@link Externalizable} is the bridge: {@code writeExternal}/{@code readExternal} delegate
 * to the {@code Writable} contract on {@code Key} (which already encodes row, cf, cq, vis,
 * timestamp, and the deletion marker compactly). The wrapper is {@link Comparable} so it can
 * drive {@code repartitionAndSortWithinPartitions} via the natural Accumulo Key ordering
 * (architecture §5.3).
 *
 * <p>Lives in {@code transform/} on purpose — it touches only the {@code data} package
 * ({@link Key}) plus pure JDK types, so the UT-6 client-bypass rule still passes.
 */
public final class SerializableKey implements Externalizable, Comparable<SerializableKey> {

    private static final long serialVersionUID = 1L;

    private Key key;

    /** Required by {@link Externalizable}; reserve for the deserialization path. */
    public SerializableKey() {
    }

    /**
     * @param key copied defensively — {@link Key}'s internal byte buffers are sometimes
     *            shared with the source iterator, so we must own a private copy.
     */
    public SerializableKey(Key key) {
        Objects.requireNonNull(key, "key");
        this.key = new Key(key);
    }

    public Key key() {
        return key;
    }

    public Text getRow() {
        return key.getRow();
    }

    @Override
    public int compareTo(SerializableKey other) {
        return key.compareTo(other.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SerializableKey other)) return false;
        return key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "SerializableKey{" + key + '}';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // ObjectOutput extends DataOutput — Key.write writes the full Writable encoding.
        key.write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        Key k = new Key();
        k.readFields(in);
        this.key = k;
    }
}
