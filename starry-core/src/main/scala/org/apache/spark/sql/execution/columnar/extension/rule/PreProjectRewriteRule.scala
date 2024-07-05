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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, Literal, NamedExpression, WindowExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.columnar.extension.plan.ColumnarSupport
import org.apache.spark.sql.execution.columnar.extension.plan.ColumnarSupport.EXPAND_PROJECT_PREFIX

import scala.collection.mutable

object PreProjectRewriteRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case aggregate: Aggregate =>
      pushdownExprsInAgg(aggregate)
    case window: Window =>
      pushdownExprsInWindow(window)
    case expand @ Expand(projections, _, child)
      if !projections.flatten.forall(isAttributeOrLit) =>

      val extraProjects = mutable.Buffer[NamedExpression]()
      projections.flatten.filter(!isAttributeOrLit(_))
      val newExpandProjections = projections.map{projList => projList.map {
        case attribute: Attribute => attribute
        case literal: Literal => literal
        case expr =>
          val proj = Alias(expr, s"${EXPAND_PROJECT_PREFIX}${extraProjects.length}")()
          extraProjects += proj
          proj.toAttribute
      }}

      expand.copy(
        projections = newExpandProjections,
        child = Project(child.output ++ extraProjects, child)
      )
  }

  private def pushdownExprsInWindow(window: Window): LogicalPlan = {
    val exprSet = new mutable.HashMap[Expression, NamedExpression]
    val pushdown = (expr: Expression) => pushdownExpr(expr, exprSet, ColumnarSupport.WIN_PROJECT_GROUP_PREFIX)
    val newPartitionSpec = window.partitionSpec.map(pushdown)
    val newOrderSpec = window.orderSpec.map { so =>
      so.copy(child = pushdown(so.child))
    }
    val newExprs = window.windowExpressions.map {
      case alias@Alias(winExpr: WindowExpression, aliasName: String) =>
        Alias(
          WindowExpression(
            winExpr.windowFunction match {
              case aggExpr: AggregateExpression =>
                aggExpr.copy(
                  aggregateFunction = aggExpr.aggregateFunction.mapChildren(pushdown).asInstanceOf[AggregateFunction])
              case other => other.mapChildren(pushdown)
            },
            winExpr.windowSpec.copy(
              partitionSpec = winExpr.windowSpec.partitionSpec.map(pushdown),
              orderSpec = winExpr.windowSpec.orderSpec.map { so => so.copy(child = pushdown(so.child)) })
          ),
          aliasName)(exprId = alias.exprId)
      case other => other
    }

    if (exprSet.isEmpty) {
      window
    } else {
      val withProj = Project(window.child.output ++ exprSet.values, window.child)
      val newWindow = Window(
        newExprs,
        newPartitionSpec,
        newOrderSpec,
        withProj
      )
      newWindow
    }
  }

  def isAttributeOrLit(e: Expression): Boolean = {
    e.isInstanceOf[AttributeReference] || e.isInstanceOf[Literal]
  }

  private def pushdownExpr(expression: Expression,
                           exprSet: mutable.HashMap[Expression, NamedExpression],
                           prefix: String): Expression = {
    expression match {
      case literal: Literal =>
        literal
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
    val pushdown = (expr: Expression) => pushdownExpr(expr, exprSet, ColumnarSupport.AGG_PROJECT_GROUP_PREFIX)
//    val newGroupings = aggregate.groupingExpressions.map(pushdown)
    val newAggExprs = aggregate.aggregateExpressions.map { aggExpr =>
      aggExpr.transformUp {
        case aggregateExpression: AggregateExpression =>
          val newAggFunc = aggregateExpression.aggregateFunction
            .mapChildren(pushdown).asInstanceOf[AggregateFunction]
          val newFilter = aggregateExpression.filter
            .map(pushdown)
          aggregateExpression.copy(aggregateFunction = newAggFunc, filter = newFilter)
      }.asInstanceOf[NamedExpression]
    }
    if (exprSet.isEmpty) {
      aggregate
    } else {
      val withProj = Project(aggregate.child.output ++ exprSet.values, aggregate.child)
      val newAgg = aggregate.copy(aggregateExpressions = newAggExprs, child = withProj)
      newAgg
    }
  }

}
