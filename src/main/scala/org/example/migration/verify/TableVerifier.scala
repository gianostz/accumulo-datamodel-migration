package org.example.migration.verify

import org.apache.accumulo.core.client.AccumuloClient
import org.apache.accumulo.core.security.Authorizations
import scala.jdk.CollectionConverters.*

object TableVerifier:

  def scanAndPrint(client: AccumuloClient, tableName: String): Unit =
    println(s"\n=== $tableName ===")
    val scanner = client.createScanner(tableName, new Authorizations())
    scanner.iterator().asScala.foreach { e =>
      val k = e.getKey
      println(s"  row=${k.getRow}  cf=${k.getColumnFamily}  cq=${k.getColumnQualifier}  val=${e.getValue}")
    }
    scanner.close()
