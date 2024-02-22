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
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  EquivalentExpressions,
  Expression,
  Literal,
  NamedExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  AggregateFunction
}
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

case class PreProjectRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
//    case aggregate: Aggregate if aggregate.getTagValue(TreeNodeTag("processed")).isEmpty =>
//      val maybeType = PhysicalAggregation.unapply(aggregate)
//      if (maybeType.isDefined) {
//        val (groupingExpressions, aggExpressions, resultExpressions, child) =
//          maybeType.get
//        val preProject = new util.ArrayList[Expression]()
//        val newAgg = aggExpressions
//          .map(_.asInstanceOf[AggregateExpression])
//          .map(processAggregateExpression(_, preProject))
//
//        val tuples = newAgg.map(e => (e.resultAttribute, alias(e)))
//        val aggNamed = tuples.map(_._2)
//        val aggToNamed = tuples.toMap
//        val newGroup = groupingExpressions.map(
//          rewrite(_, preProject, ColumnarSupport.AGG_PROJECT_GROUP_PREFIX)
//            .asInstanceOf[NamedExpression])
//
//        val a = newGroup.flatMap(_.references) ++ newAgg.flatMap(_.references)
//
//        val b = preProject.asScala
//          .map(_.asInstanceOf[NamedExpression].toAttribute)
//          .toSet
//        val attributes = a.filterNot(e => b.contains(e))
//        val preProjectExec =
//          Project(attributes ++ preProject.asScala.map(_.asInstanceOf[NamedExpression]), child)
//
//        val newResult = resultExpressions.map { e =>
//          e.transformDown {
//            case e: AttributeReference if aggToNamed.contains(e) =>
//              aggToNamed.apply(e).toAttribute
//            case other => other
//          }
//        }
//        val aggregate1 = Aggregate(newGroup, newGroup ++ aggNamed, preProjectExec)
//        aggregate1.setTagValue(TreeNodeTag("processed"), "true")
//        Project(newResult.map(_.asInstanceOf[NamedExpression]), aggregate1)
//      } else {
//        aggregate.setTagValue(TreeNodeTag("processed"), "true")
//        aggregate
//      }
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

  private[this] def alias(expr: Expression): NamedExpression = expr match {
    case expr: NamedExpression => expr
    case a: AggregateExpression if a.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
      UnresolvedAlias(a, Some(Column.generateAlias))
    case expr: Expression => Alias(expr, toPrettySQL(expr))()
  }
  private def processAggregateExpression(
      aggregateExpression: AggregateExpression,
      preProject: java.util.ArrayList[Expression]): AggregateExpression = {
    val function = aggregateExpression.aggregateFunction
    val beforeSize = preProject.size()
    val rewriteChildren =
      function.children.map(e => rewrite(e, preProject, ColumnarSupport.AGG_PROJECT_AGG_PREFIX))
    val newChildren = if (preProject.size() > beforeSize) {
      rewriteChildren
    } else {
      function.children
    }

    val newFilter = if (aggregateExpression.filter.isDefined) {
      Option.apply(
        rewrite(
          aggregateExpression.filter.get,
          preProject,
          ColumnarSupport.AGG_PROJECT_FILTER_PREFIX))
    } else {
      aggregateExpression.filter
    }
    new AggregateExpression(
      function.withNewChildren(newChildren).asInstanceOf[AggregateFunction],
      aggregateExpression.mode,
      aggregateExpression.isDistinct,
      newFilter,
      aggregateExpression.resultId)

  }

  private def rewrite(
      expression: Expression,
      preProject: java.util.ArrayList[Expression],
      prefix: String): Expression = {
    expression match {
      case attributeReference: AttributeReference =>
        attributeReference
      case other =>
        val alias =
          Alias(other, s"${prefix}${preProject.size()}")()
        preProject.add(alias)
        alias.toAttribute
    }
  }

}
