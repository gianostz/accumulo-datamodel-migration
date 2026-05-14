package org.example.migration.write

import org.apache.accumulo.core.client.rfile.RFile
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.nio.file.{Files, Path}

object RFileWriter:

  /**
   * Sorts `entries` by Accumulo key order and writes them to `outputDir/<fileName>.rf`.
   * Returns the path of the written file.
   */
  def write(fileName: String, entries: Seq[(Key, Value)], outputDir: Path): Path =
    Files.createDirectories(outputDir)
    val outFile = outputDir.resolve(s"$fileName.rf")
    val conf    = new Configuration()
    val fs      = FileSystem.getLocal(conf)
    val sorted  = entries.sortWith((a, b) => a._1.compareTo(b._1) < 0)
    val writer  = RFile.newWriter()
      .to(outFile.toString)
      .withFileSystem(fs)
      .build()
    writer.startDefaultLocalityGroup()
    sorted.foreach((k, v) => writer.append(k, v))
    writer.close()
    outFile
