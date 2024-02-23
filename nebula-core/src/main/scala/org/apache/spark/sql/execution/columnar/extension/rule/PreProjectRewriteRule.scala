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

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EquivalentExpressions, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.execution.columnar.extension.plan.ColumnarSupport.EXPAND_PROJECT_PREFIX
import org.apache.spark.sql.execution.columnar.extension.plan.ColumnarSupport

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

case class PreProjectRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case aggregate: Aggregate =>
      pushdownExprsInAgg(aggregate)
    case expand: Expand
        if !expand.projections.flatten.forall(e =>
          e.isInstanceOf[AttributeReference] || e.isInstanceOf[Literal]) =>
      val equivalentAggregateExpressions = new EquivalentExpressions
      val aggregateExpressions = expand.projections.flatten.flatMap { expr =>
        expr.collect {
          // addExpr() always returns false for non-deterministic expressions and do not add them.
          case a if !isAttributeReference(a) && !equivalentAggregateExpressions.addExpr(a) =>
            (
              a,
              Alias(
                a,
                s"${EXPAND_PROJECT_PREFIX}${equivalentAggregateExpressions.getCommonSubexpressions.size}")())
        }
      }
      val aggMap = aggregateExpressions.toMap

      val newProjections = expand.projections.map(_.map { e =>
        e.transformDown {
          case a if !isAttributeReference(a) =>
            val expression = equivalentAggregateExpressions
              .getExprState(a)
              .map(_.expr)
              .getOrElse(a)
            aggMap.apply(expression).toAttribute
        }
      })
      val a = Expand(
        newProjections,
        expand.output,
        Project(expand.child.output ++ aggregateExpressions.map(_._2), expand.child))
      a
  }

  def isAttributeReference(e: Expression): Boolean = {
    e.isInstanceOf[AttributeReference] || e.isInstanceOf[Literal]
  }

  private def pushdownExpr(expression: Expression,
                            exprSet: mutable.HashMap[Expression, NamedExpression]): Expression = {
    val prefix = ColumnarSupport.AGG_PROJECT_GROUP_PREFIX
    expression match {
      case attributeReference: AttributeReference =>
        attributeReference
      case alias: Alias =>
        exprSet.getOrElseUpdate(alias, alias).toAttribute
      case other =>
        val alias = Alias(other, s"${prefix}${exprSet.size}")()
        exprSet.getOrElseUpdate(other, alias).toAttribute
    }
  }

  private def pushdownExprsInAgg(aggregate: Aggregate): Aggregate = {
    val exprSet = new mutable.HashMap[Expression, NamedExpression]
    val newGroupings = aggregate.groupingExpressions.map(pushdownExpr(_, exprSet))
    val newAggExprs = aggregate.aggregateExpressions.map(_.transformUp {
      case aggregateExpression: AggregateExpression =>
        val newAggFunc = aggregateExpression.aggregateFunction
          .mapChildren(pushdownExpr(_, exprSet)).asInstanceOf[AggregateFunction]
        val newFilter = aggregateExpression.filter
          .map(pushdownExpr(_, exprSet))
        aggregateExpression.copy(aggregateFunction = newAggFunc, filter = newFilter)
    }.asInstanceOf[NamedExpression])
    if (exprSet.isEmpty) {
      aggregate
    } else {
      val withProj = Project(aggregate.child.output ++ exprSet.values, aggregate.child)
      val newAgg = aggregate.copy(newGroupings, newAggExprs, withProj)
      newAgg
    }
  }

}
