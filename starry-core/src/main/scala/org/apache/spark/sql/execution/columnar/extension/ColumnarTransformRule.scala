package org.apache.spark.sql.execution.columnar.extension

import org.apache.spark.sql.catalyst.expressions.{
  Ascending,
  Explode,
  Expression,
  Inline,
  SortOrder
}
import org.apache.spark.sql.catalyst.rules.{Rule, UnknownRuleId}
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, Unnest}
import org.apache.spark.sql.execution.columnar.extension.plan._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types.{AtomicType, IntegralType, LongType}

case class ColumnarTransformRule() extends Rule[SparkPlan] {

  private def canTransform(projectExec: ProjectExec): Boolean = {
    projectExec.projectList.forall(ExpressionConverter.nativeEvaluable)
  }

  private def canTransform(projectExec: FilterExec): Boolean = {
    ExpressionConverter.nativeEvaluable(projectExec.condition)
  }

  def canHashBuild(exprs: Seq[Expression]): Boolean = {
    if (exprs.forall(
//          tp => tp.dataType.isInstanceOf[AtomicType] && !tp.dataType.isInstanceOf[DecimalType])) {
          tp => tp.dataType.isInstanceOf[AtomicType])) {
      true
    } else {
      false
    }
  }

  private def canTransform(shuffledHashJoinExec: ShuffledHashJoinExec): Boolean = {
    canHashBuild(shuffledHashJoinExec.leftKeys) && canHashBuild(shuffledHashJoinExec.rightKeys)
  }

  private def canTransform(projectExec: BroadcastHashJoinExec): Boolean = {
    canHashBuild(projectExec.leftKeys) && canHashBuild(projectExec.rightKeys)

  }

  private def canTransform(projectExec: BroadcastExchangeExec): Boolean = {
    projectExec.child.isInstanceOf[ColumnarSupport]
  }

  private def canTransform(hashAggregateExec: HashAggregateExec): Boolean = {
    canHashBuild(hashAggregateExec.groupingExpressions)
  }

  private def canTransform(generateExec: GenerateExec): Boolean = {
    if (generateExec.outer) {
      return false
    }
    generateExec.generator match {
      case unnest: Unnest =>
        true
      case inline: Inline =>
        true
      case explode: Explode =>
        true
      case other => false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    val plan1 =
      plan.transformUpWithBeforeAndAfterRuleOnChildren(AlwaysProcess.fn, UnknownRuleId) {
        case (projectExec: ProjectExec, e: ProjectExec) if canTransform(projectExec) =>
          new ColumnarProjectExec(projectExec.projectList, e.child)

        case (filterExec: FilterExec, e2: FilterExec) if canTransform(filterExec) =>
          new ColumnarFilterExec(filterExec.condition, e2.child)

        case (filterExec: ShuffledHashJoinExec, e2: ShuffledHashJoinExec)
            if canTransform(filterExec) =>
          new ColumnarHashJoinExec(
            e2.leftKeys,
            e2.rightKeys,
            e2.joinType,
            e2.buildSide,
            e2.condition,
            e2.left,
            e2.right,
            e2.isSkewJoin)

        case (filterExec: BroadcastHashJoinExec, after: BroadcastHashJoinExec)
            if canTransform(filterExec) =>
          transform(after)

        case (filterExec: WindowExec, after: WindowExec) =>
          new ColumnarWindowExec(
            after.windowExpression,
            after.partitionSpec,
            after.orderSpec,
            after.child)

        case (filterExec: ExpandExec, after: ExpandExec) =>
          new ColumnarExpandExec(after.projections, after.output, after.child)


        case (_: HashAggregateExec, after: HashAggregateExec) if canTransform(after) =>
          ColumnarAggregateExec(after)

        case (_: SortExec, after: SortExec) =>
          new ColumnarSortExec(
            after.sortOrder,
            after.global,
            after.child,
            after.testSpillFrequency)

        case (_: GenerateExec, after: GenerateExec) if canTransform(after) =>
          new ColumnarGenerateExec(
            after.generator,
            after.requiredChildOutput,
            after.outer,
            after.generatorOutput,
            after.child)

        case (_: GlobalLimitExec, after: GlobalLimitExec) =>
          new ColumnarLimitExec(after.limit, false, after.child)

        case (_: LocalLimitExec, after: LocalLimitExec) =>
          new ColumnarLimitExec(after.limit, true, after.child)

        case (e, e2) =>
          e2
      }
    plan1
  }

  private def transform(bhj: BroadcastHashJoinExec): ColumnarBroadcastHashJoinExec = {
    bhj.left match {
      case exec: BroadcastExchangeExec =>
        new ColumnarBroadcastHashJoinExec(
          bhj.leftKeys,
          bhj.rightKeys,
          bhj.joinType,
          bhj.buildSide,
          bhj.condition,
          org.apache.spark.sql.execution.columnar.extension.plan
            .ColumnarBroadcastExchangeExec(exec.mode, bhj.left.children.head),
          bhj.right,
          bhj.isNullAwareAntiJoin)
      case _ =>
        new ColumnarBroadcastHashJoinExec(
          bhj.leftKeys,
          bhj.rightKeys,
          bhj.joinType,
          bhj.buildSide,
          bhj.condition,
          bhj.left,
          org.apache.spark.sql.execution.columnar.extension.plan
            .ColumnarBroadcastExchangeExec(
              bhj.right.asInstanceOf[BroadcastExchangeExec].mode,
              bhj.right.children.head),
          bhj.isNullAwareAntiJoin)
    }
  }
}
