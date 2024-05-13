package org.apache.spark.sql.execution

import com.google.common.base.Preconditions
import org.apache.spark.sql.execution.dict.ColumnDict

import scala.collection.mutable

/**
 * keep temp objects during execution
 * lifetime should be the same as query
 */
class StarryContext {

  val tempDicts: mutable.Buffer[ColumnDict] = mutable.Buffer()

  def addTempDict(columnDict: ColumnDict): Unit = {
    tempDicts += columnDict
  }

  def release(): Unit = {
    tempDicts.foreach(_.cleanup())
  }

}

object StarryContext {

  val context: ThreadLocal[StarryContext] = new ThreadLocal[StarryContext]()

  def startNewContext(): Unit = {
    clean()
    context.set(new StarryContext)
  }

  def get(): Option[StarryContext] = {
    Some(context.get())
  }

  def clean(): Unit = {
    if (context.get() != null) {
      context.get().release()
      context.remove()
    }
  }

}
