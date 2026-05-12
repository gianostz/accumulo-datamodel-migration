package org.example.poc.migration.transform;

import com.typesafe.config.Config;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Phase-7: the Spark driver that wires {@link EventTransformer} + {@link TargetTablePartitioner}
 * + {@link RFileIO} into a single transformation job (architecture §2.4).
 *
 * <h3>Pipeline</h3>
 * <ol>
 *   <li>Driver receives the source RFile paths (from {@code SourceRFileLocator}) and the split
 *       points of each target table (from {@code DatasetGenerator.splitPoints}).</li>
 *   <li>{@code parallelize(rfilePaths).flatMap(...)} reads each source RFile via
 *       {@link RFileIO#openReader} and runs {@link EventTransformer#transform} on every
 *       {@code KeyValue} — fanning out to the seven target entries (data-model §5).</li>
 *   <li>The unified RDD is cached, then per target table:
 *     <ul>
 *       <li>filter to rows for that table,</li>
 *       <li>{@code repartitionAndSortWithinPartitions} with {@link TargetTablePartitioner}
 *           (aligns each output RFile to one tablet — architecture §5.2),</li>
 *       <li>{@code mapPartitionsWithIndex} writes one RFile per non-empty partition under
 *           {@code staging/<targetTable>/part-XXXXX.rf}.</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <h3>Spark + Accumulo serialization</h3>
 * <p>Neither {@link Key} nor {@link Text} implements {@link Serializable}. The shuffle key is
 * shipped via {@link SerializableKey} (Externalizable wrapper around {@code Key}); split lists
 * inside {@link TargetTablePartitioner} are shipped via that class's custom
 * {@code writeObject}/{@code readObject} hooks. {@link Value} crosses the wire as raw
 * {@code byte[]}.
 *
 * <h3>NFR-3 / NFR-4</h3>
 * <ul>
 *   <li>No {@code now()}, no random UUIDs, no map-iteration leaking into keys — all
 *       deterministic on the inputs.</li>
 *   <li>Class lives in {@code transform/} — UT-6 fails the build if any
 *       {@code accumulo.core.client..} type other than the static-file API leaks in.</li>
 * </ul>
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

        // Wipe and recreate staging tree so re-running the wave does not pick up stale files
        // from a previous run (idempotency: same input → same output bytes — but only if the
        // output dir starts empty).
        Configuration driverHadoopConf = new Configuration();
        for (String table : splitsByTable.keySet()) {
            String tableDir = stagingBaseDir + "/" + table;
            FileSystem fs = FileSystem.get(URI.create(tableDir), driverHadoopConf);
            Path p = new Path(tableDir);
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
            fs.mkdirs(p);
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
        // Cached: filtered + reshuffled five times below (one shuffle per target table). Without
        // caching, every per-table pass would re-read every source RFile.
        entries.persist(StorageLevel.MEMORY_AND_DISK());

        // Comparator passed explicitly so the sort is anchored on the natural Accumulo Key
        // ordering (architecture §5.3) rather than on whatever default the implicit Ordering
        // resolves to. Lambda is a Comparator&Serializable — Spark requires serializable
        // closure references.
        Comparator<SerializableKey> keyOrdering =
                (Comparator<SerializableKey> & Serializable) SerializableKey::compareTo;

        Map<String, List<String>> stagingPathsByTable = new LinkedHashMap<>();
        Map<String, Long> entryCountsByTable = new LinkedHashMap<>();

        try {
            for (Map.Entry<String, ? extends Iterable<Text>> e : splitsByTable.entrySet()) {
                String table = e.getKey();
                String tableStagingDir = stagingBaseDir + "/" + table;

                List<Text> splits = new ArrayList<>();
                for (Text t : e.getValue()) {
                    splits.add(new Text(t));
                }
                TargetTablePartitioner partitioner = new TargetTablePartitioner(splits);

                // Broadcast the table name so the closure does not capture loop-local state.
                Broadcast<String> tableNameBcast = jsc.broadcast(table);
                Broadcast<String> stagingDirBcast = jsc.broadcast(tableStagingDir);

                JavaPairRDD<SerializableKey, byte[]> tableRdd = entries
                        .filter(en -> en.targetTable().equals(tableNameBcast.value()))
                        .mapToPair(en -> new Tuple2<>(en.key(), en.valueBytes()));

                JavaPairRDD<SerializableKey, byte[]> partitioned = tableRdd
                        .repartitionAndSortWithinPartitions(partitioner, keyOrdering);

                // For each partition, write one RFile (or skip if empty). Partition index → RFile
                // name; collect the per-partition entry count so the driver can build the
                // TransformResult and so callers can verify CC-1 without scanning the files.
                Function2<Integer, Iterator<Tuple2<SerializableKey, byte[]>>, Iterator<PartCount>>
                        writePartition = (idx, iter) -> {
                    if (!iter.hasNext()) {
                        return List.of(new PartCount(idx, 0L)).iterator();
                    }
                    String stagingPath = stagingDirBcast.value()
                            + "/part-" + String.format("%05d", idx) + ".rf";
                    Configuration hadoopConf = new Configuration();
                    FileSystem fs = FileSystem.get(URI.create(stagingPath), hadoopConf);
                    long count = 0;
                    try (RFileWriter w = RFileIO.openWriter(stagingPath, fs)) {
                        while (iter.hasNext()) {
                            Tuple2<SerializableKey, byte[]> kv = iter.next();
                            // RFile.append() throws on out-of-order keys (architecture §5.3),
                            // so the sortWithinPartitions invariant is enforced at write time.
                            w.append(kv._1.key(), new Value(kv._2));
                            count++;
                        }
                    }
                    return List.of(new PartCount(idx, count)).iterator();
                };

                List<PartCount> perPartCounts = partitioned
                        .mapPartitionsWithIndex(writePartition, true)
                        .collect();

                List<String> writtenPaths = new ArrayList<>();
                long totalEntries = 0;
                for (PartCount pc : perPartCounts) {
                    if (pc.count > 0) {
                        writtenPaths.add(tableStagingDir
                                + "/part-" + String.format("%05d", pc.idx) + ".rf");
                    }
                    totalEntries += pc.count;
                }
                stagingPathsByTable.put(table, writtenPaths);
                entryCountsByTable.put(table, totalEntries);
                log.info("Migration job: table {} → {} RFile(s), {} entries",
                        table, writtenPaths.size(), totalEntries);
            }
        } finally {
            entries.unpersist(false);
        }

        return new TransformResult(stagingPathsByTable, entryCountsByTable);
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

    /** Per-partition write outcome — driver folds these into the {@link TransformResult}. */
    static final class PartCount implements Serializable {
        private static final long serialVersionUID = 1L;
        final int idx;
        final long count;

        PartCount(int idx, long count) {
            this.idx = idx;
            this.count = count;
        }
    }

}
