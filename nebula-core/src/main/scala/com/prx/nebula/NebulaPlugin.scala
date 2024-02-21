package com.prx.nebula

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.columnar.extension.utils.NativeLibUtil

import java.util
import java.util.Collections

class NebulaPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new NebulaDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new NebulaExecutorPlugin()
  }
}

class NebulaDriverPlugin extends DriverPlugin with Logging {

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    LibLoader.loadLib()
    Collections.emptyMap()
  }
}

class NebulaExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    LibLoader.loadLib()
    Collections.emptyMap()
  }
}

object LibLoader {

  var isLoader = false

  def loadLib(): Unit = synchronized {
    if (isLoader) {
      return
    }
    NativeLibUtil.loadLibrary("libvelox.dylib")
    isLoader = true
  }
}
