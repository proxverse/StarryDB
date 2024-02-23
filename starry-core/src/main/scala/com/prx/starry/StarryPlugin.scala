package com.prx.starry

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.execution.columnar.extension.rule.{AggregateFunctionRewriteRule, PreProjectRewriteRule}
import org.apache.spark.sql.execution.columnar.extension.{ColumnarTransitionRule, JoinSelectionOverrides, PreRuleReplaceRowToColumnar, VeloxColumnarPostRule}
import org.apache.spark.sql.execution.columnar.extension.utils.NativeLibUtil

import java.util
import java.util.Collections

class StarryPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new StarryDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new StarryExecutorPlugin()
  }
}

class StarryDriverPlugin extends DriverPlugin with Logging {

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    LibLoader.loadLib()
    Collections.emptyMap()
  }
}

class StarryExecutorPlugin extends ExecutorPlugin with Logging {

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

object Starry {
  def starrySession(otherConf: SparkConf = new SparkConf()): SparkSession = {
    val conf = new SparkConf()
    conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    conf.set("spark.sql.adaptive.enabled", "false")
    conf.set("spark.sql.files.openCostInBytes", "100M")
    conf.set("spark.plugins", "com.prx.starry.StarryPlugin")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.sql.columnVector.offheap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "4G")
    conf.set("spark.sql.parquet.enableNestedColumnVectorizedReader", "true")
    conf.set("spark.sql.inMemoryColumnarStorage.partitionPruning", "false")
    conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
    conf.set(
      "spark.sql.cache.serializer",
      "org.apache.spark.sql.execution.columnar.cache.CachedVeloxBatchSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[2]")
      .appName("test")
      .config(otherConf)
      .withExtensions(e => e.injectOptimizerRule(AggregateFunctionRewriteRule))
      //      .withExtensions(e => e.injectOptimizerRule(AggregateProjectPushdownRewriteRule))
      .withExtensions(e => e.injectPlannerStrategy(JoinSelectionOverrides))
      .withExtensions(e =>
        e.injectColumnar(spark =>
          ColumnarTransitionRule(PreRuleReplaceRowToColumnar(spark), VeloxColumnarPostRule())))
      .getOrCreate()
    spark.sqlContext.experimental.extraOptimizations =
      Seq(PreProjectRewriteRule(spark), CollapseProject)
    spark
  }
}
