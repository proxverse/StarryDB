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
package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{
  ExistenceJoin,
  FullOuter,
  Inner,
  JoinType,
  LeftAnti,
  LeftOuter,
  LeftSemi,
  RightOuter
}
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConvert, NativeExpression}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarSupport {
  val JOIN_LEFT_PREFIX = "embedded_join_left_"
  val JOIN_RIGHT_PREFIX = "embedded_join_right_"
  val AGG_PROJECT_AGG_PREFIX = "embedded_agg_project_agg_"
  val AGG_PROJECT_FILTER_PREFIX = "embedded_agg_project_filter_"
  val AGG_PROJECT_GROUP_PREFIX = "embedded_agg_project_group_"
  val EXPAND_PROJECT_PREFIX = "embedded_expand_project_"
  val SORT_PROJECT_PREFIX = "embedded_sort_project_"
  val GENERATOR_PROJECT_PREFIX = "embedded_generator_project_"
}
trait ColumnarSupport {
  self: SparkPlan =>
  def collectPartitions(): Seq[FilePartition] = Nil

  override lazy val metrics: Map[String, SQLMetric] = {
    Map(
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "cpuWallTiming" -> SQLMetrics.createTimingMetric(sparkContext, "cpu wall timing"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputRows" -> SQLMetrics
        .createMetric(sparkContext, "number of output rows")) ++ extensionMetrics
  }

  lazy val extensionMetrics: Map[String, SQLMetric] = Map.empty

  def makePlan(operations: NativePlanBuilder): Unit = {
    children.head match {
      case c: ColumnarSupport =>
        c.makePlan(operations)
      case other =>
        throw new UnsupportedOperationException()
    }
    makePlanInternal(operations)
    nodeID = operations.nodeId()
  }

  def makePlanInternal(operations: NativePlanBuilder): Unit = {}

  def columnarInputRDDs: Seq[(String, RDD[ColumnarBatch])] = getColumnarInputRDD(children.head)

  def getColumnarInputRDD(plan: SparkPlan): Seq[(String, RDD[ColumnarBatch])] = {
    plan match {
      case c: ColumnarSupport =>
        c.columnarInputRDDs
      case _ =>
        Seq((nodeID, plan.executeColumnar()))
    }
  }
  def toNativeExpression(expression: Expression, useAlias: Boolean = true): NativeExpression = {
    val nativeExpreesion = ExpressionConvert.convertToNative(expression, true)
    if (!nativeExpreesion.isInstanceOf[NativeExpression]) {
      throw new RuntimeException(s"Failed to convert expression ${expression} to native")
    }
    nativeExpreesion.asInstanceOf[NativeExpression]
  }

  var nodeID: String = _

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    children.head.executeBroadcast()
  }

  def collectMetrics(
      metricsList: java.util.HashMap[String, (String, Map[String, SQLMetric])]): Unit = {
    children.foreach {
      case child: ColumnarSupport => child.collectMetrics(metricsList)
      case other =>
    }
    metricsList.put(nodeID, (nodeName, metrics))
  }

  def toVeloxJoinType(joinType: JoinType, buildSide: BuildSide): String = {
    joinType match {
      case Inner =>
        "INNER"
      case FullOuter =>
        "FULL"
      case LeftOuter =>
        buildSide match {
          case BuildRight =>
            "LEFT"
          case BuildLeft =>
            "RIGHT"
        }
      case RightOuter =>
        buildSide match {
          case BuildRight =>
            "RIGHT"
          case BuildLeft =>
            "LEFT"
        }
      case LeftSemi =>
        "LEFT SEMI (FILTER)"
      case LeftAnti =>
        "ANTI"
      case x: ExistenceJoin =>
        "LEFT SEMI (PROJECT)"
      case other =>
        throw new UnsupportedOperationException("")
    }
  }
}
