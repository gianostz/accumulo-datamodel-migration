package org.example.migration.transform

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.example.migration.model.{Event, EventSerializer}

object Transformer:

  val ByUser     = "events_by_user"
  val ByType     = "events_by_type"
  val ByTimeline = "events_timeline"
  val TargetTables: Seq[String] = Seq(ByUser, ByType, ByTimeline)

  /** Fans one source entry out to three target-table entries (one per table). */
  def transform(key: Key, value: Value): Seq[(String, Key, Value)] =
    val e = EventSerializer.fromEntry(key, value)
    Seq(
      (ByUser,     byUserKey(e),     EventSerializer.toValue(e)),
      (ByType,     byTypeKey(e),     EventSerializer.toValue(e)),
      (ByTimeline, timelineKey(e),   EventSerializer.toValue(e)),
    )

  private def pad19(n: Long) = f"$n%019d"

  // events_by_user  — row=userId  cf=eventType  cq=timestamp
  private def byUserKey(e: Event): Key =
    new Key(new Text(e.userId), new Text(e.eventType), new Text(pad19(e.timestamp)))

  // events_by_type  — row=eventType  cf=userId  cq=timestamp
  private def byTypeKey(e: Event): Key =
    new Key(new Text(e.eventType), new Text(e.userId), new Text(pad19(e.timestamp)))

  // events_timeline — row=reverseTimestamp  cf=userId  cq=eventType  (newest first)
  private def timelineKey(e: Event): Key =
    new Key(new Text(pad19(Long.MaxValue - e.timestamp)), new Text(e.userId), new Text(e.eventType))
