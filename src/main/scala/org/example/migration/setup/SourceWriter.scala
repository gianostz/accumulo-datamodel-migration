package org.example.migration.setup

import org.apache.accumulo.core.client.AccumuloClient
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.example.migration.model.{Event, EventSerializer}

object SourceWriter:

  /** Creates the source table, writes events via BatchWriter, then compacts to RFiles. */
  def createAndPopulate(client: AccumuloClient, tableName: String, events: Seq[Event]): Unit =
    client.tableOperations().create(tableName)
    val writer = client.createBatchWriter(tableName)
    events.foreach { e =>
      val m = new Mutation(new Text(e.userId))
      m.put(new Text(e.eventType), new Text(e.timestamp.toString), e.timestamp, EventSerializer.toValue(e))
      writer.addMutation(m)
    }
    writer.close()
    // Flush WAL data to RFiles so RFileLocator can find them
    client.tableOperations().compact(tableName, null, null, true, true)
