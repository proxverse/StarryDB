package org.apache.spark.sql.execution.dict

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}
import org.apache.spark.internal.Logging

import scala.collection.mutable


trait ColumnDictIndex {
  def table: String

  def column: String

  def encodedColumn: String

  def dict: ColumnDict
}

case class SimpleColumnDictIndex(table: String,
                                 column: String,
                                 encodedColumn: String,
                                 dict: ColumnDict) extends ColumnDictIndex

object GlobalDictRegistry extends Logging {

  private case class Key(db: String, table: String)

  private val cache: Cache[Key, mutable.Buffer[ColumnDictIndex]] =
    CacheBuilder
    .newBuilder()
    .maximumSize(100)
    .removalListener((notify: RemovalNotification[Key, mutable.Buffer[ColumnDictIndex]]) =>
      notify.getValue.foreach { v =>
        try {
          logInfo(s"remove dict ${v.table},${v.column} id ${v.dict.broadcastID}")
        }
      }
    )
    .build()


  def getColumnDictIndexes(db: String, table: String): Seq[ColumnDictIndex] = {
    Option(cache.getIfPresent(Key(db, table))).getOrElse(Seq.empty)
  }

  def findColumnDictIndex(db: String, table: String, col: String): Option[ColumnDictIndex] = {
    getColumnDictIndexes(db, table).find(_.column == col)
  }

  def invalidate(db: String, table: String): Unit = {
    cache.invalidate(Key(db, table))
    logInfo(s"column dict cache invalidated for ${db}.${table}")
  }

  def invalidateAll(): Unit = {
    cache.invalidateAll()
  }

  def register(db: String, table: String, columnDict: ColumnDictIndex): Unit = {
    cache.synchronized {
      val buf = Option(cache.getIfPresent(Key(db, table))).getOrElse(mutable.Buffer())
      buf += columnDict
      if (buf.size == 1) {
        cache.put(Key(db, table), buf)
      }
    }
  }

}
