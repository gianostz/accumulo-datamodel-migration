package org.example.poc.migration.env;

import com.typesafe.config.Config;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.example.poc.migration.transform.EventTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Lifecycle wrapper for {@link MiniAccumuloCluster}: start the in-process cluster, build an
 * {@link AccumuloClient}, create the source plus the five target tables, and apply split
 * points. Lives outside {@code transform/} on purpose — it depends on
 * {@code accumulo.core.client.*} which UT-6 forbids inside the transform package
 * (architecture §5.1).
 *
 * <p>The class is {@link AutoCloseable}: use a try-with-resources at the top of
 * {@code PoCMain} / integration tests so the cluster is reliably torn down even on failure.
 */
public final class EnvironmentSetup implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EnvironmentSetup.class);

    public static final String SOURCE_TABLE = "events_legacy";

    /** Source + 5 targets — the full set this PoC operates on. */
    public static final List<String> ALL_TABLES = List.of(
            SOURCE_TABLE,
            EventTransformer.EVENTS_BY_ID,
            EventTransformer.EVENTS_BY_USER,
            EventTransformer.EVENTS_BY_SESSION,
            EventTransformer.EVENT_COMPONENTS_SEARCHABLE,
            EventTransformer.EVENT_STATS_BY_TYPE);

    private final File baseDir;
    private final String rootPassword;

    private MiniAccumuloCluster cluster;
    private AccumuloClient client;

    public EnvironmentSetup(Path baseDir, String rootPassword) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir").toFile();
        this.rootPassword = Objects.requireNonNull(rootPassword, "rootPassword");
    }

    /** Build from the {@code accumulo.*} section of {@code application.conf}. */
    public static EnvironmentSetup fromConfig(Config config) {
        return new EnvironmentSetup(
                Path.of(config.getString("accumulo.miniDir")),
                config.getString("accumulo.rootPassword"));
    }

    /**
     * Start the cluster and connect a single shared {@link AccumuloClient}. Idempotent:
     * a second call is a no-op (lets tests call {@code start()} after the constructor
     * without caring whether someone else already did).
     */
    public void start() throws Exception {
        if (cluster != null) {
            return;
        }
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new IllegalStateException("Cannot create mini-accumulo dir: " + baseDir);
        }
        log.info("Starting MiniAccumuloCluster in {}...", baseDir);
        MiniAccumuloConfig miniConfig = new MiniAccumuloConfig(baseDir, rootPassword);
        cluster = new MiniAccumuloCluster(miniConfig);
        cluster.start();

        Properties props = cluster.getClientProperties();
        client = Accumulo.newClient().from(props).build();
        log.info("MiniAccumulo started: instance={}, zk={}",
                cluster.getInstanceName(), cluster.getZooKeepers());
    }

    public AccumuloClient client() {
        if (client == null) {
            throw new IllegalStateException("start() must be called before client()");
        }
        return client;
    }

    public String instanceName() {
        return cluster == null ? null : cluster.getInstanceName();
    }

    public String zooKeepers() {
        return cluster == null ? null : cluster.getZooKeepers();
    }

    /**
     * Create the six PoC tables if they do not exist. Safe to call after a partial previous
     * setup — existing tables are left untouched, not recreated.
     */
    public void createTables() throws Exception {
        for (String table : ALL_TABLES) {
            try {
                client.tableOperations().create(table);
                log.info("Created table {}", table);
            } catch (TableExistsException e) {
                log.info("Table {} already exists; reusing", table);
            }
        }
    }

    /**
     * Add split points to {@code table}. A {@code null} or empty set is treated as "no
     * splits requested" rather than as a request to clear existing splits.
     */
    public void applySplits(String table, SortedSet<Text> splits) throws Exception {
        if (splits == null || splits.isEmpty()) {
            log.debug("No splits to apply for {}", table);
            return;
        }
        client.tableOperations().addSplits(table, new TreeSet<>(splits));
        log.info("Applied {} split(s) to {}", splits.size(), table);
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                first = e;
            } finally {
                client = null;
            }
        }
        if (cluster != null) {
            try {
                cluster.stop();
                log.info("MiniAccumuloCluster stopped");
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            } finally {
                cluster = null;
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
