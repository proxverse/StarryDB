package org.apache.spark.sql.execution.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

object SparkLocalConfUtils {

  def setLocalConfs(localConfs: Map[String, String]): Unit = {
    Utils.tryLogNonFatalError {
      localConfs.foreach { case (k, v) =>
        SparkSession.active.sessionState.conf.setLocalProperty(k, v)
      }
    }
  }

  def withTempLocalConfs[T](localConfs: Map[String, String])(func: () => T): T = {
    var saved = Map[String, String]()
    Utils.tryLogNonFatalError {
      saved = localConfs.map{ case (k, _) => (k, SparkSession.active.sessionState.conf.getConfString(k)) }
    }

    try {
      setLocalConfs(localConfs)
      func()
    } finally {
      setLocalConfs(saved)
    }
  }

}
