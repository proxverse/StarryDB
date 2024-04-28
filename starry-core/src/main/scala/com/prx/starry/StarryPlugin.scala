package com.prx.starry

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.columnar.expressions.aggregate.BitmapCountDistinctAggFunction
import org.apache.spark.sql.execution.columnar.extension.rule.{AggregateFunctionRewriteRule, CountDistinctToBitmap, PreProjectRewriteRule}
import org.apache.spark.sql.execution.columnar.extension.utils.NativeLibUtil
import org.apache.spark.sql.execution.columnar.extension.{ColumnarTransitionRule, JoinSelectionOverrides, PreRuleReplaceRowToColumnar, VeloxColumnarPostRule}
import org.apache.spark.sql.internal.{SQLConf, StarryConf}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import java.util
import java.util.Collections
import scala.reflect.ClassTag

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
    LibLoader.loadLib(sc.getConf)
    Collections.emptyMap()
  }
}

class StarryExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    LibLoader.loadLib(ctx.conf())
    Collections.emptyMap()
  }
}

object LibLoader {

  var isLoader = false
  private implicit lazy val formats = Serialization.formats(NoTypeHints)
  def loadLib(conf: SparkConf): Unit = synchronized {
    if (isLoader) {
      return
    }
    try {
      NativeLibUtil.loadLibrary("libstarry.dylib")

      val str = StarryConf.getAllConfJson(conf, "spark.sql.starry")
      if (str.nonEmpty) {
        NativeLibUtil.init(str)
      }
    } catch {
      case runtimeException: RuntimeException =>
        NativeLibUtil.loadLibrary("libstarry.so")
    }
    isLoader = true
  }
}

object Starry {

  private def functionDescription[T <: Expression : ClassTag]
  (name: String,
   since: Option[String] = None
  ): (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, since)
    val newBuilder = (expressions: Seq[Expression]) => {
      val expr = builder(expressions)
      expr
    }
    (FunctionIdentifier(name), expressionInfo, newBuilder)
  }


  def injectExtensions(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectOptimizerRule(_ => AggregateFunctionRewriteRule)
    sparkSessionExtensions.injectPlannerStrategy(JoinSelectionOverrides)
    sparkSessionExtensions.injectColumnar(spark =>
      ColumnarTransitionRule(PreRuleReplaceRowToColumnar(spark), VeloxColumnarPostRule()))

    sparkSessionExtensions.injectFunction(
      functionDescription[BitmapCountDistinctAggFunction]("bitmap_count_distinct"))
  }

  def extraOptimizations: Seq[Rule[LogicalPlan]] =
    CountDistinctToBitmap ::
    AggregateFunctionRewriteRule ::
    PreProjectRewriteRule ::
    CollapseProject :: Nil


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
//      conf.set("spark.sql.starry.columnar.forceShuffledHashJoin", "false")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[2]")
      .appName("test")
      .config(otherConf)
      .withExtensions(e => injectExtensions(e))
      .getOrCreate()
    spark.sqlContext.experimental.extraOptimizations ++= extraOptimizations
    spark
  }
}
