package org.example.poc.migration.locate;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Strategy A from architecture §2.3: discover the physical RFile paths backing a table by
 * reading {@code accumulo.metadata}. Lives outside {@code transform/} on purpose — it uses
 * {@code AccumuloClient}, which UT-6 forbids inside the transform package (architecture §5.1).
 *
 * <p>Workflow:
 * <ol>
 *   <li>Caller invokes {@link #compactAndWait(String)} (or the row-bounded variant) to
 *       consolidate the tablets to a stable set of files before reading them.</li>
 *   <li>Caller invokes {@link #locate(String)} (or {@link #locate(String, Text, Text)}) to
 *       enumerate the resulting {@link RFileRef}s.</li>
 *   <li>The driver hands the paths to Spark, which reads them via the static-file RFile API
 *       in {@code transform/RFileIO} — no cluster connection from executors.</li>
 * </ol>
 *
 * <p>Path note: in MiniAccumulo, the returned paths are local filesystem URIs (e.g.
 * {@code file:/tmp/.../A0000abc.rf}) rather than HDFS URIs. Pass them through as strings —
 * {@code FileSystem.get(Configuration)} resolves either form via the URI scheme.
 */
public final class SourceRFileLocator {

    private static final Logger log = LoggerFactory.getLogger(SourceRFileLocator.class);

    /**
     * One RFile in one tablet of the located table.
     *
     * @param path                full path to the file (URI form, scheme-qualified)
     * @param sizeBytes           on-disk size as recorded in the metadata table
     * @param numEntries          number of {@code KeyValue} pairs recorded in metadata
     * @param tabletPrevEndRow    inclusive lower bound of the tablet; {@code null} for the
     *                            first tablet (covers from negative infinity)
     * @param tabletEndRow        inclusive upper bound of the tablet; {@code null} for the
     *                            last (default) tablet (covers to positive infinity)
     */
    public record RFileRef(
            String path,
            long sizeBytes,
            long numEntries,
            String tabletPrevEndRow,
            String tabletEndRow) {

        public RFileRef {
            Objects.requireNonNull(path, "path");
        }
    }

    private final AccumuloClient client;

    public SourceRFileLocator(AccumuloClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    /** Force a synchronous full-table compaction so the metadata reflects a stable file set. */
    public void compactAndWait(String tableName) throws Exception {
        compactAndWait(tableName, null, null);
    }

    /**
     * Force a synchronous compaction over the given row range. {@code null} bounds extend the
     * range in that direction; both null is equivalent to {@link #compactAndWait(String)}.
     */
    public void compactAndWait(String tableName, Text startRow, Text endRow) throws Exception {
        CompactionConfig cfg = new CompactionConfig().setFlush(true).setWait(true);
        if (startRow != null) {
            cfg.setStartRow(startRow);
        }
        if (endRow != null) {
            cfg.setEndRow(endRow);
        }
        log.info("Compacting {} (start={}, end={})", tableName, startRow, endRow);
        client.tableOperations().compact(tableName, cfg);
    }

    /** Locate every RFile backing {@code tableName}. */
    public List<RFileRef> locate(String tableName) throws Exception {
        return locate(tableName, null, null);
    }

    /**
     * Locate the RFiles backing tablets that intersect the inclusive row range
     * {@code [startRow, endRow]}. A {@code null} bound is unbounded on that side.
     *
     * <p>The unit of returned files is the <i>tablet</i>: a tablet whose range overlaps even
     * partially with the requested range contributes <i>all</i> of its files. Callers that need
     * finer-grained range filtering must filter inside their KV-level pipeline. This matches
     * the Strategy A description in architecture §2.3 — RFile granularity is a tablet, not a
     * row.
     */
    public List<RFileRef> locate(String tableName, Text startRow, Text endRow) throws Exception {
        String tableIdStr = client.tableOperations().tableIdMap().get(tableName);
        if (tableIdStr == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        TableId tableId = TableId.of(tableIdStr);

        Map<String, Text> tabletPrevEnd = new HashMap<>();
        Map<String, List<DataFileEntry>> tabletFiles = new HashMap<>();

        try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
            scanner.setRange(TabletsSection.getRange(tableId));
            scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
            TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);

            for (Map.Entry<Key, Value> entry : scanner) {
                Key k = entry.getKey();
                String row = k.getRow().toString();
                if (isPrevRowColumn(k)) {
                    Text prev = TabletColumnFamily.decodePrevEndRow(entry.getValue());
                    tabletPrevEnd.put(row, prev);
                } else if (k.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
                    StoredTabletFile stf = new StoredTabletFile(k.getColumnQualifier().toString());
                    DataFileValue dfv = new DataFileValue(entry.getValue().get());
                    tabletFiles.computeIfAbsent(row, r -> new ArrayList<>())
                            .add(new DataFileEntry(stf.getPathStr(), dfv.getSize(), dfv.getNumEntries()));
                }
            }
        }

        List<RFileRef> result = new ArrayList<>();
        for (Map.Entry<String, List<DataFileEntry>> e : tabletFiles.entrySet()) {
            String tabletRow = e.getKey();
            Text prev = tabletPrevEnd.get(tabletRow);
            String prevStr = prev == null ? null : prev.toString();
            String endStr = extractTabletEndRow(tabletRow, tableId);
            if (!tabletOverlapsRange(prevStr, endStr, startRow, endRow)) {
                continue;
            }
            for (DataFileEntry f : e.getValue()) {
                result.add(new RFileRef(f.path, f.size, f.entries, prevStr, endStr));
            }
        }
        // Stable order keeps Spark's parallelize input reproducible across runs (NFR-3 spirit).
        result.sort(Comparator.comparing(RFileRef::path));
        log.info("Located {} RFile(s) in {} (range start={}, end={})",
                result.size(), tableName, startRow, endRow);
        return result;
    }

    private static boolean isPrevRowColumn(Key k) {
        return k.getColumnFamily().equals(TabletColumnFamily.NAME)
                && k.getColumnQualifier().toString().equals(TabletColumnFamily.PREV_ROW_QUAL);
    }

    /**
     * The metadata row for a tablet is {@code <tableId>;<endRow>} for a normal tablet and
     * {@code <tableId><} for the default (last) tablet. Returns the endRow or {@code null}
     * for the default tablet.
     */
    private static String extractTabletEndRow(String tabletRow, TableId tableId) {
        String prefix = tableId.canonical();
        if (!tabletRow.startsWith(prefix) || tabletRow.length() <= prefix.length()) {
            return null;
        }
        char sep = tabletRow.charAt(prefix.length());
        if (sep == '<') {
            return null;
        }
        if (sep == ';') {
            return tabletRow.substring(prefix.length() + 1);
        }
        return null;
    }

    /**
     * Tablet covers {@code (prev, end]}; requested filter is inclusive on both ends. The two
     * exclusion conditions:
     * <ul>
     *   <li>{@code rangeStart > tabletEnd} — the request begins past the tablet's last row.</li>
     *   <li>{@code rangeEnd <= tabletPrev} — the request ends at or before the tablet's lower
     *       (open) boundary. The {@code <=} is what makes the lower bound open: a request that
     *       ends exactly at {@code prevEndRow} does not touch the tablet.</li>
     * </ul>
     * {@code null} on either side means "unbounded in that direction" and never excludes.
     */
    private static boolean tabletOverlapsRange(String tabletPrev, String tabletEnd,
                                               Text rangeStart, Text rangeEnd) {
        if (rangeStart != null && tabletEnd != null
                && rangeStart.toString().compareTo(tabletEnd) > 0) {
            return false;
        }
        if (rangeEnd != null && tabletPrev != null
                && rangeEnd.toString().compareTo(tabletPrev) <= 0) {
            return false;
        }
        return true;
    }

    private record DataFileEntry(String path, long size, long entries) {}
}
