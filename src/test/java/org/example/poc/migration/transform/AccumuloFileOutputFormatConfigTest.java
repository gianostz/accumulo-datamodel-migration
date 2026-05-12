package org.example.poc.migration.transform;

import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * UT-7 — Driver-side configuration of {@link AccumuloFileOutputFormat}.
 *
 * <p>The migration job replaces our hand-rolled {@code RFile.newWriter()} loop with the
 * battle-tested {@link AccumuloFileOutputFormat} driven from Spark's
 * {@code saveAsNewAPIHadoopFile}. This test pins down two properties of the driver-side wiring
 * that the new write path depends on:
 *
 * <ol>
 *   <li><b>The fluent builder properly stores the output path</b> on the Hadoop {@link Job},
 *       so Spark/Hadoop find it on the executor.</li>
 *   <li><b>Construction and {@code checkOutputSpecs} require no cluster contact.</b>
 *       Instantiating {@link AccumuloFileOutputFormat} and calling its setup-time hooks against
 *       a local-only {@link Job} must succeed with no Zookeeper / TabletServer connection.
 *       Disassembly of Accumulo 2.1.2 confirms this; the test serves as a tripwire that
 *       fails if a future upgrade adds a cluster dependency.</li>
 * </ol>
 *
 * <p>End-to-end write through {@code AccumuloFileOutputFormat} (RecordWriter → RFile file on
 * disk) is exercised by {@code MigrationJobIT} against a real {@code MiniAccumuloCluster}; UT-7
 * intentionally stays a pure unit test (no MiniAccumulo, no Spark).
 */
class AccumuloFileOutputFormatConfigTest {

    @Test
    void configure_storesOutputPath_onTheStandardHadoopKey(@TempDir java.nio.file.Path tmp)
            throws Exception {
        Job job = Job.getInstance(new Configuration(), "ut7-outputPath");
        Path outPath = new Path(tmp.toUri().toString() + "/staging");

        AccumuloFileOutputFormat.configure()
                .outputPath(outPath)
                .store(job);

        // Driver-side: Spark / Hadoop read this key on the executor. If the fluent builder ever
        // stops setting it, AccumuloFileOutputFormat.getRecordWriter() would resolve work files
        // against a null base and fail in confusing ways.
        assertEquals(outPath, FileOutputFormat.getOutputPath(job));
    }

    @Test
    void configure_acceptsCompressionAndBlockSize_withoutClusterContact(
            @TempDir java.nio.file.Path tmp) throws Exception {
        // No MiniAccumulo, no Zookeeper running. If any of these calls reach out to a cluster
        // the JVM would hang / throw on connection; passing here is the proof of bypass.
        Job job = Job.getInstance(new Configuration(), "ut7-options");
        Path outPath = new Path(tmp.toUri().toString() + "/staging");

        assertDoesNotThrow(() -> AccumuloFileOutputFormat.configure()
                .outputPath(outPath)
                .compression("gz")
                .dataBlockSize(64L * 1024)
                .fileBlockSize(1L * 1024 * 1024)
                .indexBlockSize(128L * 1024)
                .replication(1)
                .store(job));
    }

    @Test
    void outputFormat_constructibleAndCheckOutputSpecs_makesNoClusterCall(
            @TempDir java.nio.file.Path tmp) throws Exception {
        Job job = Job.getInstance(new Configuration(), "ut7-checkOutputSpecs");
        Path outPath = new Path(tmp.toUri().toString() + "/staging-fresh");

        AccumuloFileOutputFormat.configure().outputPath(outPath).store(job);

        // Construction is the most basic check: a static initializer that attempted to load
        // an AccumuloClient would fail here.
        AccumuloFileOutputFormat outputFormat = new AccumuloFileOutputFormat();
        assertNotNull(outputFormat);

        // FileOutputFormat.checkOutputSpecs requires the output path NOT to exist. We never
        // created it, so the call must succeed silently and without touching any cluster.
        assertDoesNotThrow(() -> outputFormat.checkOutputSpecs(job));
    }
}
