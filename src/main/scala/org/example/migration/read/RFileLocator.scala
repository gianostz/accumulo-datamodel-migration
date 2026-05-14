package org.example.migration.read

import org.apache.accumulo.core.client.AccumuloClient
import org.apache.accumulo.core.data.{Range => ARange}
import org.apache.accumulo.core.metadata.{MetadataTable, StoredTabletFile}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import scala.jdk.CollectionConverters.*

object RFileLocator:

  /**
   * Returns absolute URIs for every RFile backing `tableName`, read from the
   * accumulo.metadata table.
   */
  def getFiles(client: AccumuloClient, tableName: String): Seq[String] =
    val tableId = client.tableOperations().tableIdMap().get(tableName)

    // Metadata tablet rows are "<tableId>;<endRow>" for split tablets and
    // "<tableId><" for the default (null end-row) tablet. Since ';' (0x3B) < '<' (0x3C),
    // the inclusive range ["<tableId>;", "<tableId><"] covers every tablet of the table.
    val scanner = client.createScanner(MetadataTable.NAME, new Authorizations())
    scanner.fetchColumnFamily(new Text("file"))
    scanner.setRange(new ARange(tableId + ";", true, tableId + "<", true))

    // In Accumulo 2.1.x the 'file' column qualifier is a serialized StoredTabletFile,
    // not a bare path — StoredTabletFile.getPathStr() yields the full absolute URI.
    val paths = scanner.iterator().asScala.map { e =>
      new StoredTabletFile(e.getKey.getColumnQualifier.toString).getPathStr
    }.toSeq
    scanner.close()
    paths
