package org.example.migration.ingest

import org.apache.accumulo.core.client.AccumuloClient

object BulkImporter:

  /** Creates `tableName` and bulk-imports all RFiles from `dir` into it. */
  def importRFiles(client: AccumuloClient, tableName: String, dir: String): Unit =
    client.tableOperations().create(tableName)
    client.tableOperations()
      .importDirectory(dir)
      .to(tableName)
      .tableTime(false)
      .load()
