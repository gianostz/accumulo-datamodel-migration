package org.example.migration.read

import org.apache.accumulo.core.client.rfile.RFile
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.jdk.CollectionConverters.*
import java.net.URI

object RFileReader:

  /** Reads all entries from the given RFile URIs without going through a TabletServer. */
  def readEntries(filePaths: Seq[String]): Seq[(Key, Value)] =
    if filePaths.isEmpty then return Seq.empty
    val conf = new Configuration()
    val fs   = FileSystem.get(new URI(filePaths.head), conf)
    val scanner = RFile.newScanner()
      .from(filePaths.toArray: _*)
      .withFileSystem(fs)
      .build()
    val entries = scanner.iterator().asScala
      .map(e => (new Key(e.getKey), new Value(e.getValue)))
      .toSeq
    scanner.close()
    entries
