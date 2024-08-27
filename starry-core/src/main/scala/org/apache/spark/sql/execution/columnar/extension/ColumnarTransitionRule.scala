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
package org.apache.spark.sql.execution.columnar.extension

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.internal.StarryConf.isStarryEnabled
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.columnar.extension.plan._
import org.apache.spark.sql.execution.columnar.extension.rule.{
  CollapseProjectExec,
  SingleAggregateRule
}
import org.apache.spark.sql.execution.columnar.jni.NativeQueryContext
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.{
  ColumnarRule,
  ColumnarToRowExec,
  RowToColumnarExec,
  SparkPlan
}

case class ColumnarTransitionRule(pre: Rule[SparkPlan], post: Rule[SparkPlan])
    extends ColumnarRule {

  override def preColumnarTransitions: Rule[SparkPlan] = pre

  override def postColumnarTransitions: Rule[SparkPlan] = post
}

case class PreRuleReplaceRowToColumnar(session: SparkSession)
    extends Rule[SparkPlan]
    with Logging {

  // Tracks whether the given input plan's top parent is exchange.
  private var isTopParentExchange: Boolean = false
  // Tracks whether the columnar rule is called through AQE.
  private var isAdaptiveContext: Boolean = false
  // This is an empirical value, may need to be changed for supporting other versions of spark.
  private val aqeStackTraceIndex = 13

  private var originalPlan: SparkPlan = _

  @transient private lazy val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  def preOverrides(): List[SparkSession => Rule[SparkPlan]] = {
    List(
      (spark: SparkSession) =>
        org.apache.spark.sql.execution.columnar.extension.rule.ConvertParquetFileFormat(spark),
      (_: SparkSession) => ColumnarRewriteRule(),
      (_: SparkSession) => SingleAggregateRule(),
      (_: SparkSession) => ColumnarTransformRule(),
      (_: SparkSession) => OptimizeExchange(),
      (_: SparkSession) => OptimizeSort(),
      (_: SparkSession) => CollapseProjectExec,
    )
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = {

    var overridden: SparkPlan = plan
    val startTime = System.nanoTime()
    val traceElements = Thread.currentThread.getStackTrace
    assert(
      traceElements.length > aqeStackTraceIndex,
      s"The number of stack trace elements is expected to be more than $aqeStackTraceIndex")
    // ApplyColumnarRulesAndInsertTransitions is called by either QueryExecution or
    // AdaptiveSparkPlanExec. So by checking the stack trace, we can know whether
    // columnar rule will be applied in adaptive execution context. This part of code
    // needs to be carefully checked when supporting higher versions of spark to make
    // sure the calling stack has not been changed.
    this.isAdaptiveContext = traceElements(aqeStackTraceIndex).getClassName
      .equals(AdaptiveSparkPlanExec.getClass.getName)
//     Holds the original plan for possible entire fallback.
    originalPlan = plan
//    logInfo(s"preColumnarTransitions preOverriden plan:\n${plan.toString}")
    preOverrides().foreach { r =>
      overridden = r(session)(overridden)
      planChangeLogger.logRule(r(session).ruleName, plan, overridden)
    }
    logInfo(s"preColumnarTransitions afterOverriden plan:\n${overridden.toString}")
    logInfo(s"preTransform SparkPlan took: ${(System.nanoTime() - startTime) / 1000000.0} ms.")
    overridden
  }

  override def apply(plan: SparkPlan): SparkPlan =
    if (!isStarryEnabled) {
      plan
    } else {
      try {
        NativeQueryContext.clear()
        new NativeQueryContext()
        replaceWithColumnarPlan(plan)
      } catch {
        case e =>
          logError("Error for appy to columnar", e)
          plan
      }
    }
}

case class VeloxColumnarPostRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {

    val after = plan transformDown {
      case rc: ColumnarSupport
          if !rc.isInstanceOf[ColumnarInputAdapter] && rc
            .asInstanceOf[SparkPlan]
            .children
            .exists(e => !e.isInstanceOf[ColumnarSupport]) =>
        val plan1 = rc.asInstanceOf[SparkPlan]
        plan1.withNewChildren(plan1.children.map {
          case e: ColumnarSupport =>
            e
          case other =>
            ColumnarInputAdapter(other)
        })
      case RowToColumnarExec(child: ColumnarShuffleExchangeExec) =>
        child
      case RowToColumnarExec(child: StreamingColumnarShuffleExchangeExec) =>
        child
      case rc: ColumnarToRowExec if isStarryEnabled && rc.child.isInstanceOf[ColumnarSupport] =>
        new VeloxColumnarToRowExec(
          new ColumnarEngineExec(rc.child)(
            ColumnarEngineExec.transformStageCounter.incrementAndGet()))
      case rc: ColumnarToRowExec if isStarryEnabled && !rc.child.isInstanceOf[ColumnarSupport] =>
        new VeloxColumnarToRowExec(rc.child)
      case rc: RowToColumnarExec =>
        new RowToVeloxColumnarExec(rc.child)
      case _ @ColumnarBroadcastExchangeExec(mode, child: ColumnarSupport) =>
        ColumnarBroadcastExchangeExec(
          mode,
          ColumnarEngineExec(child)(ColumnarEngineExec.transformStageCounter.incrementAndGet()))
      case plan => plan
    }
    after
  }

}
