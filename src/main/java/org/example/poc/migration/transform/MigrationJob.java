package org.example.poc.migration.transform;

import com.typesafe.config.Config;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Spark driver that wires {@link EventTransformer} into a transformation job whose write side is
 * Accumulo's {@code AccumuloFileOutputFormat} (architecture §2.4).
 *
 * <h3>Pipeline</h3>
 * <ol>
 *   <li>Driver receives the source RFile paths (from {@code SourceRFileLocator}) and the names
 *       of the five target tables. Split points are <i>not</i> consumed here — bulk import will
 *       re-split produced RFiles on the fly (architecture §5.2). The {@code splitsByTable}
 *       parameter is preserved for compatibility with existing callers; only its keyset is
 *       used.</li>
 *   <li>{@code parallelize(rfilePaths).flatMap(...)} reads each source RFile via
 *       {@link RFileIO#openReader} and runs {@link EventTransformer#transform} on every
 *       {@code KeyValue} — fanning out to the seven target entries (data-model §5).</li>
 *   <li>The unified RDD is cached, then per target table:
 *     <ul>
 *       <li>filter to rows for that table,</li>
 *       <li>{@code sortByKey} on the natural Accumulo {@code Key} ordering (architecture §5.3),</li>
 *       <li>convert {@code (SerializableKey, byte[])} → {@code (Key, Value)} via a partition-local
 *           {@code mapToPair} (no shuffle),</li>
 *       <li>{@code saveAsNewAPIHadoopFile(staging/&lt;table&gt;, Key.class, Value.class,
 *           AccumuloFileOutputFormat.class, conf)} — one RFile per Spark task.</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <h3>NFR-4</h3>
 * <p>The write path is {@link AccumuloFileOutputFormat} from {@code accumulo-hadoop-mapreduce}.
 * Its {@code getRecordWriter()} reads only the Hadoop {@code Configuration} — confirmed by
 * disassembling Accumulo 2.1.2 — and opens an RFile writer at the task's default work file. No
 * {@code AccumuloClient}, {@code BatchScanner}, or {@code BatchWriter} is instantiated and no
 * cluster connection is opened. UT-6 fails the build if any of those types creep in.
 *
 * <h3>NFR-3</h3>
 * <p>No {@code now()}, no random UUIDs, no map-iteration leaking into keys — all deterministic on
 * the inputs.
 */
public final class MigrationJob {

    private static final Logger log = LoggerFactory.getLogger(MigrationJob.class);

    private final String sparkMaster;
    private final String appName;

    public MigrationJob(String sparkMaster, String appName) {
        this.sparkMaster = Objects.requireNonNull(sparkMaster, "sparkMaster");
        this.appName = Objects.requireNonNull(appName, "appName");
    }

    public static MigrationJob fromConfig(Config config) {
        return new MigrationJob(
                config.getString("spark.master"),
                config.getString("spark.appName"));
    }

    /**
     * Run the transformation in a freshly-created {@link JavaSparkContext}. The context is
     * started and stopped by this call. Use {@link #runWith} to share an existing context
     * (e.g. across tests in a class).
     */
    public TransformResult run(
            List<String> sourceRFilePaths,
            Map<String, ? extends Iterable<Text>> splitsByTable,
            String stagingBaseDir) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setMaster(sparkMaster)
                .setAppName(appName);
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            return runWith(jsc, sourceRFilePaths, splitsByTable, stagingBaseDir);
        }
    }

    /**
     * Run the transformation against an externally-managed {@link JavaSparkContext}. Tests use
     * this to amortize the (slow) Spark startup across multiple cases.
     *
     * @param splitsByTable retained for source-compatibility with callers; only the keyset
     *                      (target-table names) is used. Splits are no longer consumed by the
     *                      job — see architecture §5.2.
     */
    public TransformResult runWith(
            JavaSparkContext jsc,
            List<String> sourceRFilePaths,
            Map<String, ? extends Iterable<Text>> splitsByTable,
            String stagingBaseDir) throws IOException {
        Objects.requireNonNull(jsc, "jsc");
        Objects.requireNonNull(sourceRFilePaths, "sourceRFilePaths");
        Objects.requireNonNull(splitsByTable, "splitsByTable");
        Objects.requireNonNull(stagingBaseDir, "stagingBaseDir");

        if (sourceRFilePaths.isEmpty()) {
            log.info("No source RFiles to migrate — returning empty result");
            Map<String, List<String>> emptyPaths = new LinkedHashMap<>();
            Map<String, Long> emptyCounts = new LinkedHashMap<>();
            for (String table : splitsByTable.keySet()) {
                emptyPaths.put(table, List.of());
                emptyCounts.put(table, 0L);
            }
            return new TransformResult(emptyPaths, emptyCounts);
        }

        // Wipe per-table staging dirs. Must NOT pre-create them — Hadoop's
        // FileOutputFormat.checkOutputSpecs throws if the output path already exists.
        Configuration driverHadoopConf = new Configuration();
        for (String table : splitsByTable.keySet()) {
            String tableDir = stagingBaseDir + "/" + table;
            FileSystem fs = FileSystem.get(URI.create(tableDir), driverHadoopConf);
            Path p = new Path(tableDir);
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
        }

        int numFiles = sourceRFilePaths.size();
        JavaRDD<String> pathsRdd = jsc.parallelize(sourceRFilePaths, numFiles);

        // ---- Read + fan out (1 → 7) ------------------------------------------------------
        // Each task opens its source RFile via the static API (no client connection — NFR-4).
        // EventTransformer is instantiated per-task (no captured driver state), keeping the
        // closure free of non-serializable references.
        JavaRDD<TableEntryBytes> entries = pathsRdd.flatMap(path -> {
            Configuration hadoopConf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(path), hadoopConf);
            EventTransformer xformer = new EventTransformer();
            List<TableEntryBytes> out = new ArrayList<>();
            try (RFileIO.Reader reader = RFileIO.openReader(path, fs)) {
                for (Map.Entry<Key, Value> e : reader) {
                    for (TargetEntry te : xformer.transform(e.getKey(), e.getValue())) {
                        out.add(new TableEntryBytes(
                                te.targetTable(),
                                new SerializableKey(te.key()),
                                te.value().get()));
                    }
                }
            }
            return out.iterator();
        });
        // Cached: filtered, counted, and re-sorted five times below. Without caching, every
        // per-table pass would re-read every source RFile.
        entries.persist(StorageLevel.MEMORY_AND_DISK());

        // Comparator passed explicitly so sortByKey is anchored on the natural Accumulo Key
        // ordering (architecture §5.3). Lambda is Comparator&Serializable — Spark requires
        // serializable closure references.
        Comparator<SerializableKey> keyOrdering =
                (Comparator<SerializableKey> & Serializable) SerializableKey::compareTo;

        Map<String, List<String>> stagingPathsByTable = new LinkedHashMap<>();
        Map<String, Long> entryCountsByTable = new LinkedHashMap<>();

        try {
            for (String table : splitsByTable.keySet()) {
                String tableStagingDir = stagingBaseDir + "/" + table;

                Broadcast<String> tableNameBcast = jsc.broadcast(table);

                JavaPairRDD<SerializableKey, byte[]> tablePairs = entries
                        .filter(en -> en.targetTable().equals(tableNameBcast.value()))
                        .mapToPair(en -> new Tuple2<>(en.key(), en.valueBytes()));

                // count() is the cheapest way to get the per-table fan-out cardinality for
                // CC-1 — the upstream RDD is cached, so this is a single pass over the
                // in-memory entries. saveAsNewAPIHadoopFile does not return a record count.
                long count = tablePairs.count();
                entryCountsByTable.put(table, count);

                if (count > 0) {
                    JavaPairRDD<SerializableKey, byte[]> sorted = tablePairs.sortByKey(keyOrdering);

                    // (SerializableKey, byte[]) → (Key, Value): partition-local map, no shuffle.
                    // Key/Value are not Serializable, so they may only appear in the RDD AFTER
                    // the last shuffle (the sortByKey above). Within a single task they cross
                    // no wire — the iterator hands them straight to AccumuloFileOutputFormat.
                    JavaPairRDD<Key, Value> kvRdd = sorted.mapToPair(t ->
                            new Tuple2<>(t._1.key(), new Value(t._2)));

                    // A fresh Configuration per table. AccumuloFileOutputFormat reads only its
                    // own block-size / compression keys from this conf, with sensible Accumulo
                    // defaults if unset. Spark sets the output path from the first argument.
                    Configuration writeConf = new Configuration();

                    kvRdd.saveAsNewAPIHadoopFile(
                            tableStagingDir,
                            Key.class,
                            Value.class,
                            AccumuloFileOutputFormat.class,
                            writeConf);
                }

                // Enumerate produced RFiles for the wave report. AccumuloFileOutputFormat names
                // them part-r-NNNNN.rf; _SUCCESS and other Hadoop sidecars are filtered out.
                List<String> rfilePaths = listRFiles(tableStagingDir, driverHadoopConf);
                stagingPathsByTable.put(table, rfilePaths);

                log.info("Migration job: table {} → {} RFile(s), {} entries",
                        table, rfilePaths.size(), count);
            }
        } finally {
            entries.unpersist(false);
        }

        return new TransformResult(stagingPathsByTable, entryCountsByTable);
    }

    private static List<String> listRFiles(String tableDir, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(tableDir), conf);
        Path dirPath = new Path(tableDir);
        if (!fs.exists(dirPath)) {
            return List.of();
        }
        FileStatus[] statuses = fs.listStatus(dirPath, p -> p.getName().endsWith(".rf"));
        List<String> out = new ArrayList<>(statuses.length);
        for (FileStatus st : statuses) {
            out.add(st.getPath().toString());
        }
        Collections.sort(out);
        return out;
    }

    /**
     * Spark-shippable element of the per-table fan-out RDD. Holds the target table, the
     * Spark-shippable Key wrapper, and the value as raw bytes (Value is just a byte[] holder).
     */
    static final class TableEntryBytes implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String targetTable;
        private final SerializableKey key;
        private final byte[] valueBytes;

        TableEntryBytes(String targetTable, SerializableKey key, byte[] valueBytes) {
            this.targetTable = targetTable;
            this.key = key;
            this.valueBytes = valueBytes;
        }

        String targetTable() { return targetTable; }
        SerializableKey key() { return key; }
        byte[] valueBytes() { return valueBytes; }
    }

}
