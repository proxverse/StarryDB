package org.apache.spark.listener

import org.apache.spark.{SparkContext, StarryEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorRemoved}

class StarryAppListener extends SparkListener with Logging {

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    StarryEnv.get.memoryManager.master.removeExecutor(executorRemoved.executorId)
    StarryEnv.get.shuffleManagerMaster.removeExecutor(executorRemoved.executorId)
  }
}

object StarryAppListener {

  def register(sc: SparkContext): Unit = {
    sc.addSparkListener(new StarryAppListener())
  }
}
