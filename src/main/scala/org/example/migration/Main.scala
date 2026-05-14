package org.example.migration

import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.accumulo.core.client.Accumulo
import org.apache.accumulo.core.data.{Key, Value}
import org.example.migration.model.Event
import org.example.migration.setup.SourceWriter
import org.example.migration.read.{RFileLocator, RFileReader}
import org.example.migration.transform.Transformer
import org.example.migration.write.RFileWriter
import org.example.migration.ingest.BulkImporter
import org.example.migration.verify.TableVerifier

import java.nio.file.Files

@main def run(): Unit =
  val workDir = Files.createTempDirectory("accumulo-migration-poc")
  println(s"[setup]  work dir: $workDir")

  val mini = new MiniAccumuloCluster(workDir.toFile, "secret")
  mini.start()
  println("[setup]  mini-accumulo started")

  val client = Accumulo.newClient().from(mini.getClientProperties).build()
  try
    // --- seed data -----------------------------------------------------------
    val events = Seq(
      Event("user1", "click",    1000L, """{"page":"home"}"""),
      Event("user1", "purchase", 2000L, """{"item":"book"}"""),
      Event("user2", "click",    3000L, """{"page":"cart"}"""),
      Event("user2", "view",     4000L, """{"item":"phone"}"""),
      Event("user3", "click",    5000L, """{"page":"home"}"""),
    )
    val sourceTable = "events_source"
    SourceWriter.createAndPopulate(client, sourceTable, events)
    println(s"[write]  ${events.size} events written and compacted into '$sourceTable'")

    // --- locate RFiles -------------------------------------------------------
    val rfilePaths = RFileLocator.getFiles(client, sourceTable)
    println(s"[locate] found ${rfilePaths.size} RFile(s)")
    rfilePaths.foreach(p => println(s"         $p"))

    // --- read RFiles directly (no TabletServer) ------------------------------
    val sourceEntries = RFileReader.readEntries(rfilePaths)
    println(s"[read]   ${sourceEntries.size} entries read from source RFiles")

    // --- transform: 1 source entry → 3 target-table entries -----------------
    val grouped: Map[String, Seq[(Key, Value)]] =
      sourceEntries
        .flatMap((k, v) => Transformer.transform(k, v))
        .groupBy(_._1)
        .view.mapValues(_.map(t => (t._2, t._3))).toMap
    grouped.foreach((t, es) => println(s"[xform]  ${es.size} entries staged for '$t'"))

    // --- write each group to its own RFile -----------------------------------
    val outputRoot = workDir.resolve("output")
    val importDirs = grouped.map { (tableName, entries) =>
      val rfile = RFileWriter.write(tableName, entries, outputRoot.resolve(tableName))
      println(s"[write]  RFile written: $rfile")
      tableName -> rfile.getParent.toString
    }

    // --- bulk import ---------------------------------------------------------
    importDirs.foreach { (tableName, dir) =>
      BulkImporter.importRFiles(client, tableName, dir)
      println(s"[import] '$tableName' bulk-imported from $dir")
    }

    // --- verify --------------------------------------------------------------
    Transformer.TargetTables.foreach(TableVerifier.scanAndPrint(client, _))

  finally
    client.close()
    mini.stop()
    println("\n[done]")
