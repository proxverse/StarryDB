package org.apache.spark.sql.execution.columnar.extension

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  ArraysZip,
  AttributeReference,
  Expression,
  Generator,
  GetStructField,
  Inline,
  Literal,
  NamedExpression,
  SortOrder,
  Stack
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.expressions.Unnest
import org.apache.spark.sql.execution.columnar.extension.plan.ColumnarSupport
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

case class ColumnarRewriteRule() extends Rule[SparkPlan] {

  def hasExpression(keyExprs: Seq[Expression]): Boolean = {
    !keyExprs.forall(_.isInstanceOf[AttributeReference])
  }
  private def needTransform(projectExec: ShuffledHashJoinExec): Boolean = {
    hasExpression(projectExec.leftKeys) || hasExpression(projectExec.rightKeys)
  }

  private def needTransform(projectExec: BroadcastHashJoinExec): Boolean = {
    hasExpression(projectExec.leftKeys) || hasExpression(projectExec.rightKeys)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    try {
      val plan1 =
        plan.transformDown {
          case shuffledHashJoinExec: ShuffledHashJoinExec
              if needTransform(shuffledHashJoinExec) =>
            // join push 到exchange 后面,减少 exchange的压力,就是看起来有点奇怪
            val (leftKeys, left) = if (!hasExpression(shuffledHashJoinExec.leftKeys)) {
              (shuffledHashJoinExec.leftKeys, shuffledHashJoinExec.left)
            } else {
              extractToProject(
                ColumnarSupport.JOIN_LEFT_PREFIX,
                shuffledHashJoinExec.leftKeys,
                shuffledHashJoinExec.left)
            }

            val (rightKeys, right) = if (!hasExpression(shuffledHashJoinExec.rightKeys)) {
              (shuffledHashJoinExec.rightKeys, shuffledHashJoinExec.right)
            } else {
              extractToProject(
                ColumnarSupport.JOIN_RIGHT_PREFIX,
                shuffledHashJoinExec.rightKeys,
                shuffledHashJoinExec.right)
            }
            ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              shuffledHashJoinExec.joinType,
              shuffledHashJoinExec.buildSide,
              shuffledHashJoinExec.condition,
              left,
              right,
              shuffledHashJoinExec.isSkewJoin)
          case shuffledHashJoinExec: BroadcastHashJoinExec
              if needTransform(shuffledHashJoinExec) =>
            // extractToProject push到 exchange 下面
            val (leftKeys, left) = if (!hasExpression(shuffledHashJoinExec.leftKeys)) {
              (shuffledHashJoinExec.leftKeys, shuffledHashJoinExec.left)
            } else {
              shuffledHashJoinExec.left match {
                case broadcastExchangeExec: BroadcastExchangeExec =>
                  val tp = extractToProject(
                    ColumnarSupport.JOIN_LEFT_PREFIX,
                    shuffledHashJoinExec.leftKeys,
                    broadcastExchangeExec.child)
                  (tp._1, BroadcastExchangeExec(broadcastExchangeExec.mode, tp._2))
                case _ =>
                  extractToProject(
                    ColumnarSupport.JOIN_LEFT_PREFIX,
                    shuffledHashJoinExec.leftKeys,
                    shuffledHashJoinExec.left)
              }
            }

            val (rightKeys, right) = if (!hasExpression(shuffledHashJoinExec.rightKeys)) {
              (shuffledHashJoinExec.rightKeys, shuffledHashJoinExec.right)
            } else {
              shuffledHashJoinExec.right match {
                case broadcastExchangeExec: BroadcastExchangeExec =>
                  val tp = extractToProject(
                    ColumnarSupport.JOIN_RIGHT_PREFIX,
                    shuffledHashJoinExec.rightKeys,
                    broadcastExchangeExec.child)
                  (tp._1, BroadcastExchangeExec(broadcastExchangeExec.mode, tp._2))
                case _ =>
                  extractToProject(
                    ColumnarSupport.JOIN_RIGHT_PREFIX,
                    shuffledHashJoinExec.rightKeys,
                    shuffledHashJoinExec.right)
              }
            }
            BroadcastHashJoinExec(
              leftKeys,
              rightKeys,
              shuffledHashJoinExec.joinType,
              shuffledHashJoinExec.buildSide,
              shuffledHashJoinExec.condition,
              left,
              right,
              shuffledHashJoinExec.isNullAwareAntiJoin)
          case sortExec: SortExec if hasExpression(sortExec.sortOrder.map(_.child)) =>
            val (newExpr, newChild) = extractToProject(
              ColumnarSupport.SORT_PROJECT_PREFIX,
              sortExec.sortOrder.map(_.child),
              sortExec.child)
            ProjectExec(
              sortExec.child.output,
              SortExec(
                sortExec.sortOrder
                  .zip(newExpr)
                  .map(tp => tp._1.withNewChildren(Seq(tp._2)).asInstanceOf[SortOrder]),
                sortExec.global,
                newChild,
                sortExec.testSpillFrequency))

          case aggExec: HashAggregateExec =>
            rewriteAggregate(aggExec)

          case projectExec: ProjectExec if projectExec.projectList.isEmpty =>
            projectExec.child

          case generateExec: GenerateExec
              if !generateExec.outer && hasExpression(generateExec.generator.children) =>
            val afterRewrite = rewriteGenerate(generateExec)
            // rewrite array may be unresolved
            if (!afterRewrite.generator.resolved) {
              generateExec
            } else {
              val (newExpr, newChild) = extractToProject(
                ColumnarSupport.GENERATOR_PROJECT_PREFIX,
                afterRewrite.generator.children,
                afterRewrite.child,
                true)
              GenerateExec(
                afterRewrite.generator.withNewChildren(newExpr).asInstanceOf[Generator],
                afterRewrite.requiredChildOutput,
                afterRewrite.outer,
                afterRewrite.generatorOutput,
                newChild)
            }
          case other => other
        }
      plan1
    } catch {
      case t: Throwable =>
        logError("e", t)
        t.getCause
        plan
      case other => plan
    }

  }

  private def rewriteGenerate(generateExec: GenerateExec): GenerateExec = {
    val newGenerator = generateExec.generator match {
      case stack: Stack =>
        val fields1 = stack.elementSchema.fields
        val children = stack.children
        val numRows = children.head.eval().asInstanceOf[Int]
        val numFields = Math.ceil((children.length - 1.0) / numRows).toInt
        val columns = Range(0, numRows).map { row =>
          val fields =
            Range(0, numFields).map { field =>
              val index = row * numFields + field + 1
              if (index >= children.length) {
                new Column(new Literal(null, fields1.apply(field).dataType))
              } else {
                new Column(children.apply(index))
              }
            }
          struct(fields: _*)
        }
        val expr1 = array(columns: _*).expr
        new Inline(expr1).asInstanceOf[Generator]
      case inline @ Inline(array: ArraysZip) =>
        new Unnest(array.children).asInstanceOf[Generator]
      case other =>
        other
    }

    GenerateExec(
      newGenerator,
      generateExec.requiredChildOutput,
      generateExec.outer,
      generateExec.generatorOutput,
      generateExec.child)

  }

  private def extractToProject(
      prefix: String,
      keys: Seq[Expression],
      plan: SparkPlan,
      extractLiteral: Boolean = false): (Seq[Expression], SparkPlan) = {
    val expressionsWithWindowFunctions = keys
    val regularExpressions = plan.output
    val extractedExprBuffer = new ArrayBuffer[NamedExpression]()
    def extractExpr(expr: Expression): Expression = expr match {
      case aggregateExpression: AggregateExpression =>
        val newChildren = aggregateExpression.aggregateFunction.children.map(extractExpr)
        val newFilter = if (aggregateExpression.filter.isDefined) {
          Option.apply(extractExpr(aggregateExpression.filter.get))
        } else {
          Option.empty
        }
        val expression = aggregateExpression.aggregateFunction.withNewChildren(newChildren)
        new AggregateExpression(
          expression.asInstanceOf[AggregateFunction],
          aggregateExpression.mode,
          aggregateExpression.isDistinct,
          newFilter,
          aggregateExpression.resultId)

      case ne: NamedExpression =>
        // If a named expression is not in regularExpressions, add it to
        // extractedExprBuffer and replace it with an AttributeReference.
        val missingExpr = Set(expr) -- (regularExpressions ++ extractedExprBuffer)
        if (missingExpr.nonEmpty) {
          extractedExprBuffer += ne
        }
        // alias will be cleaned in the rule CleanupAliases
        ne.toAttribute

      case e: Expression if !extractLiteral && e.foldable =>
        e // No need to create an attribute reference if it will be evaluated as a Literal.
      case e: Expression =>
        // For other expressions, we extract it and replace it with an AttributeReference (with
        // an internal column name, e.g. "_w0").
        val withName = Alias(e, s"${prefix}${extractedExprBuffer.length}")()
        extractedExprBuffer += withName
        withName.toAttribute
    }
    val afterRewrite = expressionsWithWindowFunctions.map(extractExpr)
    if (afterRewrite.isEmpty) {
      return (afterRewrite, plan)
    }
    val newProject = new ProjectExec(regularExpressions ++ extractedExprBuffer, plan)
    (afterRewrite, newProject)
  }

  private def applyPostProjectToAgg(aggExec: HashAggregateExec): SparkPlan = {
    val isAllOfAttr = aggExec.resultExpressions.forall(_.isInstanceOf[AttributeReference])
    if (isAllOfAttr) {
      aggExec
    } else {
      val newHash = aggExec.copy(
        resultExpressions = aggExec.groupingExpressions ++ aggExec.aggregateAttributes)
      ProjectExec(aggExec.resultExpressions, newHash)
    }
  }

  // TODO remove this
  private def applyPreProjectToAgg(aggExec: HashAggregateExec): HashAggregateExec = {
    val merelyReference = (expr: Expression) => expr.isInstanceOf[AttributeReference]
    // no need for extra projection
    if (aggExec.groupingExpressions.forall(merelyReference) &&
        aggExec.aggregateExpressions.flatMap(_.children).forall(merelyReference)) {
      return aggExec
    }

    // extract projects
    val (newExprs, preProject) = extractToProject(
      ColumnarSupport.AGG_PROJECT_AGG_PREFIX,
      aggExec.groupingExpressions ++ aggExec.aggregateExpressions,
      aggExec.child)
    val (newAggExprs, newGroupings) = newExprs.partition(_.isInstanceOf[AggregateExpression])
    aggExec.copy(
      groupingExpressions = newGroupings.map(_.asInstanceOf[NamedExpression]),
      aggregateExpressions = newAggExprs.map(_.asInstanceOf[AggregateExpression]),
      child = preProject)
  }

  private def rewriteAggregate(aggExec: HashAggregateExec): SparkPlan = {
    aggExec.aggregateExpressions.headOption.map(_.mode) match {
      case Some(Final) =>
        applyPostProjectToAgg(aggExec)
      case Some(Partial) =>
        applyPreProjectToAgg(aggExec)
      case Some(Complete) | None =>
        applyPostProjectToAgg(applyPreProjectToAgg(aggExec))
      case _ =>
        aggExec
    }
  }
}
