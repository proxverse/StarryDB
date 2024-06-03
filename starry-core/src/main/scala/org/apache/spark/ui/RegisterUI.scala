package org.apache.spark.ui

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart}

object RegisterUI {
  def register(sc: SparkContext): Unit = {
    if (sc.uiWebUrl.nonEmpty) {
      sc.addSparkListener(new SparkListener {
        override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
          Thread.sleep(10 * 1000)
          sc.ui.foreach(_.attachTab(new StarryMemoryUI(sc.ui.get)))
        }
      })

    }
  }

}
