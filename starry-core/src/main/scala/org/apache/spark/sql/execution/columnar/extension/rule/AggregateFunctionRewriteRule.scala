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

package org.apache.spark.sql.execution.columnar.extension.rule

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, ApproximatePercentile, Average, AverageBase, CollectList, CollectSet, Count, HyperLogLogPlusPlus, Percentile, Sum}
import org.apache.spark.sql.catalyst.expressions.{And, Cast, CreateStruct, Expression, If, IsNotNull, IsNull, Literal, Or, SupportQueryContext}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.StarryConf
import org.apache.spark.sql.execution.columnar.expressions.HLLAdapter
import org.apache.spark.sql.execution.columnar.expressions.aggregate.BitmapCountDistinctAggFunction
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

object AggExpr {
  def unapply(expr: AggregateExpression): Option[AggregateFunction] = expr match {
    case aggExpr: AggregateExpression =>
      Some(aggExpr.aggregateFunction)
    case _ =>
      None
  }
}

case class AggregateFunctionRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!StarryConf.isStarryEnabled) {
      return plan
    }
    plan.transformUp {
      case a: Aggregate =>
        a.transformExpressions {
          case agg @ AggExpr(bitmapCountD: BitmapCountDistinctAggFunction) =>
            agg.copy(aggregateFunction = new NativeFunctionPlaceHolder(bitmapCountD))
          case agg @ AggExpr(collectList: CollectList) =>
            agg.copy(
              aggregateFunction = new NativeFunctionPlaceHolder(
                collectList,
                collectList.child :: Nil,
                agg.dataType,
                "array_agg"))
          case agg @ AggExpr(collectSet @ CollectSet(child, _, _)) =>
            val newFilter = if (agg.filter.isDefined) {
              And(agg.filter.get, IsNotNull(child))
            } else {
              IsNotNull(child)
            }
            agg.copy(
              aggregateFunction = new NativeFunctionPlaceHolder(
                collectSet,
                child :: Nil,
                agg.dataType.asNullable,
                "set_agg"),
              filter = Option.apply(newFilter))

          case agg @ AggExpr(Average(child, _)) if child.dataType.isInstanceOf[DecimalType] =>
            agg.copy(
              aggregateFunction = new NativeFunctionPlaceHolder(
                agg.aggregateFunction,
                Cast(child, agg.dataType) :: Nil,
                agg.dataType))

          case agg @ AggExpr(percentile: ApproximatePercentile) =>
            val accuracy =
              1.0 / percentile.accuracyExpression.eval().asInstanceOf[Number].longValue
            agg.copy(
              aggregateFunction = new NativeFunctionPlaceHolder(
                percentile,
                percentile.children.take(2) ++ Seq(lit(accuracy).expr),
                percentile.dataType,
                "approx_percentile"))

          case hllExpr @ AggExpr(hll: HyperLogLogPlusPlus)
              if StarryConf.isStarryEnabled &&
                !hasDistinctAggregateFunc(a) && isDataTypeSupported(hll.child.dataType) =>
            AggregateExpression(
              HLLAdapter(
                hll.child,
                Literal(hll.relativeSD),
                hll.mutableAggBufferOffset,
                hll.inputAggBufferOffset),
              hllExpr.mode,
              hllExpr.isDistinct,
              hllExpr.filter,
              hllExpr.resultId)

          case ca @ AggregateExpression(count: Count, _, false, _, _)
              if count.children.size > 1 =>
            val nullableChildren = count.children.filter(_.nullable)
            val merge = if (nullableChildren.isEmpty) {
              Literal(1, IntegerType)
            } else {
              If(
                nullableChildren.map(IsNull).reduce(Or),
                Literal(0, IntegerType),
                Literal(1, IntegerType))
            }
            AggregateExpression(Sum(merge), ca.mode, ca.isDistinct, ca.filter, ca.resultId)
          case ca @ AggregateExpression(count: Count, _, false, _, _)
              if count.children.size > 1 =>
            val nullableChildren = count.children.filter(_.nullable)
            val merge = if (nullableChildren.isEmpty) {
              Literal(1, IntegerType)
            } else {
              If(
                nullableChildren.map(IsNull).reduce(Or),
                Literal(0, IntegerType),
                Literal(1, IntegerType))
            }
            AggregateExpression(Sum(merge), ca.mode, ca.isDistinct, ca.filter, ca.resultId)
          case ca @ AggregateExpression(count: Count, _, true, _, _) if count.children.size > 1 =>
            AggregateExpression(
              Count(Seq(CreateStruct.create(count.children))),
              ca.mode,
              ca.isDistinct,
              ca.filter,
              ca.resultId)
        }
    }
  }

  private def hasDistinctAggregateFunc(agg: Aggregate): Boolean = {
    agg.aggregateExpressions
      .flatMap(_.collect { case ae: AggregateExpression => ae })
      .exists(_.isDistinct)
  }

  private def isDataTypeSupported(dataType: DataType): Boolean = {
    // HLL in velox only supports below data types. we should not offload HLL to velox, if
    // child's data type is not supported. This prevents the case only partail agg is fallbacked.
    // As spark and velox have different HLL binary formats, HLL binary generated by spark can't
    // be parsed by velox, it would cause the error: 'Unexpected type of HLL'.
    dataType match {
      case BooleanType => true
      case ByteType => true
      case _: CharType => true
      case DateType => true
      case DoubleType => true
      case FloatType => true
      case IntegerType => true
      case LongType => true
      case ShortType => true
      case StringType => true
      case _ => false
    }
  }
}
