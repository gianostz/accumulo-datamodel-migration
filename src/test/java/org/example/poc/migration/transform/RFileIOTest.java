package org.example.poc.migration.transform;

import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * UT-5 — {@link RFileIO} round-trip + ordering invariant.
 *
 * <p>Writes N ordered {@code (Key, Value)} pairs to a local RFile via {@link RFileIO#openWriter}
 * and reads them back via {@link RFileIO#openReader} — asserts the same N pairs in the same
 * order (Keys equal, Value bytes equal).
 *
 * <p>Also verifies architecture §5.3: the writer rejects out-of-order appends. This is the
 * invariant Spark's {@code sortWithinPartitions} relies on — if it ever silently accepted
 * unsorted input, we would produce malformed RFiles only to discover it at bulk-import time.
 */
class RFileIOTest {

    private static Key key(String row) {
        return new Key(row, "cf", "cq", "", 1_000L);
    }

    private static Value val(String s) {
        return new Value(s.getBytes(StandardCharsets.UTF_8));
    }

    private static FileSystem localFs() throws IOException {
        return FileSystem.getLocal(new Configuration());
    }

    @Test
    void writeThenRead_returnsSamePairsInSameOrder(@TempDir Path tmp) throws Exception {
        FileSystem fs = localFs();
        String path = tmp.resolve("roundtrip.rf").toUri().toString();

        // Five sorted entries — Accumulo natural Key ordering on the row first.
        List<Map.Entry<Key, Value>> input = new ArrayList<>();
        input.add(Map.entry(key("a"), val("alpha")));
        input.add(Map.entry(key("b"), val("bravo")));
        input.add(Map.entry(key("c"), val("charlie")));
        input.add(Map.entry(key("d"), val("delta")));
        input.add(Map.entry(key("e"), val("echo")));

        try (RFileWriter w = RFileIO.openWriter(path, fs)) {
            for (Map.Entry<Key, Value> e : input) {
                w.append(e.getKey(), e.getValue());
            }
        }

        List<Map.Entry<Key, Value>> readBack = new ArrayList<>();
        try (RFileIO.Reader r = RFileIO.openReader(path, fs)) {
            for (Map.Entry<Key, Value> e : r) {
                // Iterator yields views over the same underlying buffer — copy the bytes.
                readBack.add(Map.entry(new Key(e.getKey()), new Value(e.getValue().get())));
            }
        }

        assertEquals(input.size(), readBack.size(), "entry count");
        for (int i = 0; i < input.size(); i++) {
            assertEquals(input.get(i).getKey(), readBack.get(i).getKey(), "key @ " + i);
            assertEquals(
                    input.get(i).getKey().getTimestamp(),
                    readBack.get(i).getKey().getTimestamp(),
                    "ts @ " + i);
            assertArrayEquals(
                    input.get(i).getValue().get(),
                    readBack.get(i).getValue().get(),
                    "value @ " + i);
        }
    }

    @Test
    void writerRejects_outOfOrderAppend(@TempDir Path tmp) throws Exception {
        // Architecture §5.3: RFile.append() throws if keys are not in ascending order.
        // This is the invariant Spark sortWithinPartitions relies on — if it silently
        // accepted unsorted input we would produce malformed RFiles, only to discover at
        // bulk-import time.
        FileSystem fs = localFs();
        String path = tmp.resolve("unordered.rf").toUri().toString();

        try (RFileWriter w = RFileIO.openWriter(path, fs)) {
            w.append(key("b"), val("first"));
            assertThrows(IllegalArgumentException.class,
                    () -> w.append(key("a"), val("second")),
                    "writer must reject a key that sorts before the previously appended key");
        }
    }

    @Test
    void emptyRFile_roundTrips_asZeroEntries(@TempDir Path tmp) throws Exception {
        FileSystem fs = localFs();
        String path = tmp.resolve("empty.rf").toUri().toString();

        try (RFileWriter w = RFileIO.openWriter(path, fs)) {
            // no appends
        }

        int count = 0;
        try (RFileIO.Reader r = RFileIO.openReader(path, fs)) {
            for (Map.Entry<Key, Value> ignored : r) {
                count++;
            }
        }
        assertEquals(0, count);
    }
}
