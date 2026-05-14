package org.example.migration.model

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text

// Change fields here to adapt the domain model.
// Only EventSerializer below needs updating when the schema changes.
case class Event(userId: String, eventType: String, timestamp: Long, payload: String)

object EventSerializer:

  // Source table layout: row=userId  cf=eventType  cq=timestamp  val=payload
  def toKey(e: Event): Key =
    new Key(new Text(e.userId), new Text(e.eventType), new Text(e.timestamp.toString))

  def toValue(e: Event): Value = new Value(e.payload)

  def fromEntry(k: Key, v: Value): Event = Event(
    userId    = k.getRow.toString,
    eventType = k.getColumnFamily.toString,
    timestamp = k.getColumnQualifier.toString.toLong,
    payload   = v.toString,
  )
