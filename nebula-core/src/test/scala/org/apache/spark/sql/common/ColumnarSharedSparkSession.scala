/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.common
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.execution.columnar.extension.{
  ColumnarTransitionRule,
  JoinSelectionOverrides,
  PreRuleReplaceRowToColumnar,
  VeloxColumnarPostRule
}
import org.apache.spark.sql.execution.columnar.extension.rule.{
  AggregateFunctionRewriteRule,
  PreProjectRewriteRule
}

trait ColumnarSharedSparkSession extends org.apache.spark.sql.test.SharedSparkSession {
  private var _spark: SparkSession = null

  override implicit def spark: SparkSession = _spark

  override def initializeSession(): Unit = {
    val conf = sparkConf
    conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    conf.set("spark.sql.adaptive.enabled", "false")
    conf.set("spark.sql.files.openCostInBytes", "100M")
    conf.set("spark.plugins", "com.prx.nebula.NebulaPlugin")
    conf.set("spark.gluten.sql.columnar.backend.lib", "velox")
    conf.set("spark.gluten.sql.columnar.broadcastexchange", "true")
    conf.set("spark.gluten.sql.columnar.filescan", "false")
    conf.set("spark.gluten.sql.columnar.broadcastJoin", "true")
    // conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.sql.columnVector.offheap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "4G")
    conf.set("spark.gluten.sql.enable.native.parquetReader", "true")
    conf.set("spark.sql.parquet.enableNestedColumnVectorizedReader", "true")
    conf.set("spark.sql.testkey", "true")
    conf.set("spark.gluten.enabled", "false")
//    conf.set("spark.sql.optimizer.rewriteCountDistinct", "false")
    conf.set("spark.gluten.rewriterColumnarExpression.enabled", "false")
    conf.set("spark.sql.inMemoryColumnarStorage.partitionPruning", "false")
    conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "true")
    conf.set(
      "spark.sql.cache.serializer",
      "org.apache.spark.sql.execution.columnar.cache.CachedVeloxBatchSerializer")
//    conf.set("spark.gluten.sql.native.parquetReader.async", "false")
    _spark = SparkSession
      .builder()
      .config(conf)
      .master("local[1]")
      .withExtensions(e => e.injectOptimizerRule(AggregateFunctionRewriteRule))
//      .withExtensions(e => e.injectOptimizerRule(AggregateProjectPushdownRewriteRule))
      .withExtensions(e => e.injectPlannerStrategy(JoinSelectionOverrides))
      .withExtensions(e =>
        e.injectColumnar(spark =>
          ColumnarTransitionRule(PreRuleReplaceRowToColumnar(spark), VeloxColumnarPostRule())))
      .appName("test-sql-context")
      .getOrCreate()
    _spark.sqlContext.experimental.extraOptimizations = Seq(new PreProjectRewriteRule(spark))
    _spark
  }
  override def sqlContext: SQLContext = _spark.sqlContext

}
