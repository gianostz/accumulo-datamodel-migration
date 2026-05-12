package org.example.poc.migration.transform;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Static-file RFile read / write wrapper. Belongs in {@code transform/} — opens no connection
 * to an Accumulo cluster; reads and writes RFiles via Hadoop {@link FileSystem}.
 *
 * <h3>UT-6 carve-outs honoured here</h3>
 * <p>NFR-4 forbids cluster-connecting Accumulo client types. The static-file RFile API
 * legitimately lives in {@code org.apache.accumulo.core.client.rfile} and
 * {@code RFile.newScanner().build()} returns {@code org.apache.accumulo.core.client.Scanner}
 * — that Scanner is bound to an RFile path, not a cluster. Both are explicitly allowed by
 * {@link ClientBypassArchTest}; {@code BatchScanner}, {@code BatchWriter}, and
 * {@code AccumuloClient} stay banned.
 *
 * <p>The {@link Scanner} reference is intentionally contained: the public {@link Reader} view
 * exposes only {@link Iterable} + {@link AutoCloseable}, so callers in {@code transform/} need
 * not name the type at all.
 */
public final class RFileIO {

    private RFileIO() {
    }

    /**
     * Open an RFile for sequential read. The returned {@link Reader} is {@link AutoCloseable}.
     *
     * @param path absolute filesystem path of the RFile (e.g. {@code file:/tmp/x.rf})
     * @param fs   Hadoop {@link FileSystem} that resolves {@code path}
     */
    public static Reader openReader(String path, FileSystem fs) {
        Scanner scanner = RFile.newScanner()
                .from(path)
                .withFileSystem(fs)
                .build();
        return new Reader(scanner);
    }

    /**
     * Open an RFile for write. Keys must be appended in ascending natural order
     * (architecture §5.3) — the underlying writer throws otherwise. The caller is responsible
     * for closing the writer to finalize the file footer.
     */
    public static RFileWriter openWriter(String path, FileSystem fs) throws IOException {
        return RFile.newWriter()
                .to(path)
                .withFileSystem(fs)
                .build();
    }

    /**
     * Iterable + AutoCloseable view over an RFile. Hides the {@link Scanner} return type so
     * callers in {@code transform/} can read entries without naming the carve-out type.
     */
    public static final class Reader implements Iterable<Map.Entry<Key, Value>>, AutoCloseable {
        private final Scanner scanner;

        private Reader(Scanner scanner) {
            this.scanner = scanner;
        }

        @Override
        public Iterator<Map.Entry<Key, Value>> iterator() {
            return scanner.iterator();
        }

        @Override
        public void close() {
            scanner.close();
        }
    }
}
