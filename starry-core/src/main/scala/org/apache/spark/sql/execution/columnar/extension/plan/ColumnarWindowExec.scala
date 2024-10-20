package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.expressions.ExpressionConverter
import org.apache.spark.sql.execution.columnar.extension.plan.BoundType.{Following, Preceding}
import org.apache.spark.sql.execution.columnar.extension.plan.ColumnarWindowExec.toNativeFrame
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarWindowExec(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
    extends WindowExec(windowExpression, partitionSpec, orderSpec, child)
    with ColumnarSupport {

  override def supportsColumnar: Boolean =
    true
  // Disable code generation
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColumnarWindowExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarWindowExec =
    new ColumnarWindowExec(windowExpression, partitionSpec, orderSpec, newChild)

  override def collectPartitions(): Seq[FilePartition] = {
    child.asInstanceOf[ColumnarSupport].collectPartitions()
  }

  val nativeAggMethodMapping =
    Map[String, String]("collect_list" -> "array_agg", "collect_set" -> "set_agg")

  def withNewAggName(originName: String): String = {
    if (nativeAggMethodMapping.contains(originName)) {
      nativeAggMethodMapping.apply(originName)
    } else {
      originName
    }
  }

  override def makePlanInternal(operations: NativePlanBuilder): Unit = {
    var count = 0
    val windowFunctionJsons = windowExpression.map { windowExpr =>
      val aliasExpr = windowExpr.asInstanceOf[Alias]
      val wExpression = aliasExpr.child.asInstanceOf[WindowExpression]
      wExpression.windowFunction match {
        case aggExpression: AggregateExpression =>
          val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          val aggregateFunc = aggExpression.aggregateFunction
          val functionCall = toNativeExpressionJson(aggregateFunc)
          val frameJson = toNativeFrame(operations, frame)

          operations.windowFunction(functionCall, frameJson, false)

        case wf @ (RowNumber() | Rank(_) | DenseRank(_) | CumeDist() | PercentRank(_)) =>
          val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          val frameJson = toNativeFrame(operations, frame)
          val expression = toNativeExpressionJson(wf)
          operations.windowFunction(expression, frameJson, false)
        case wf @ (Lead(_, _, _, _) | Lag(_, _, _, _)) =>
          val offset_wf = wf.asInstanceOf[FrameLessOffsetWindowFunction]
          val frame = offset_wf.frame.asInstanceOf[SpecifiedWindowFrame]
          val frameJson = toNativeFrame(operations, frame)
          val expression = toNativeExpressionJson(wf)
          wf match {
            case lead: Lead =>
              operations.windowFunction(expression, frameJson, lead.ignoreNulls)
            case lag: Lag =>
              operations.windowFunction(expression, frameJson, lag.ignoreNulls)
            case lead: NthValue =>
              operations.windowFunction(expression, frameJson, lead.ignoreNulls)
          }
        case wf: NthValue =>
          val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          val frameJson = toNativeFrame(operations, frame)
          val expression = toNativeExpressionJson(wf)
          operations.windowFunction(expression, frameJson, wf.ignoreNulls)
        case _ =>
          throw new UnsupportedOperationException(
            "unsupported window function type: " +
              wExpression.windowFunction)
      }
    }.toArray

    val partitionsKeys = partitionSpec.map(toNativeExpressionJson).toArray
    val orderKeys = orderSpec.map(_.child).map(toNativeExpressionJson).toArray
    val windowColumnNames = windowExpression
      .map(_.toAttribute)
      .map(e => ExpressionConverter.toNativeAttrIdName(e))
      .toArray
    operations.window(
      partitionsKeys,
      orderKeys,
      orderSpec.toArray,
      windowColumnNames,
      windowFunctionJsons,
      false)

  }

}

sealed abstract class BoundType(val value: Int, val name: String)

object BoundType {
  case object UnboundedPreceding extends BoundType(0, "UNBOUNDED PRECEDING")
  case object Preceding extends BoundType(1, "PRECEDING")
  case object CurrentRow extends BoundType(2, "CURRENT ROW")
  case object Following extends BoundType(3, "FOLLOWING")
  case object UnboundedFollowing extends BoundType(4, "UNBOUNDED FOLLOWING")

  // Optionally, if you need to iterate over cases or do other collection-like operations
  val values = List(UnboundedPreceding, Preceding, CurrentRow, Following, UnboundedFollowing)

  // Optionally, if you need to access a case by its value
  def fromValue(value: Int): Option[BoundType] = values.find(_.value == value)

  // Optionally, if you need to access a case by its name
  def fromName(name: String): Option[BoundType] = values.find(_.name == name)
}

object ColumnarWindowExec {

  case class WindowFrame(
      frameType: FrameType,
      startType: BoundType,
      startValue: Long,
      endType: BoundType,
      endValue: Long) {

    override def toString: String = {
      val startValueJson = startType match {
        case Preceding | Following =>
          // ref  velox: updateFrameBounds
          if (startValue > (Int.MaxValue - 100000)) {
            throw new UnsupportedOperationException("To big range")
          }
          if (frameType == RangeFrame && startValue > 0) {
            throw new UnsupportedOperationException("unsupported literal + range")
          }
          s"""
             |,"startValue":{"value":{"type":"BIGINT","value":${startValue}},
             |"type":{"type":"BIGINT","name":"Type"},"name":"ConstantTypedExpr"}
             |
             |
             |""".stripMargin
        case _ => ""
      }

      val endValueJson = endType match {
        case Preceding | Following =>
          if (endValue > (Int.MaxValue - 100000)) {
            throw new UnsupportedOperationException("To big range")
          }
          if (frameType == RangeFrame && endValue > 0) {
            throw new UnsupportedOperationException("unsupported literal + range")
          }
          s"""
             |,"endValue":{"value":{"type":"BIGINT","value":${endValue}},
             |"type":{"name":"Type","type":"BIGINT"},"name":"ConstantTypedExpr"}
             |
             |
             |""".stripMargin
        case _ => ""
      }
      s"""
        |{"endType":"${endType.name}" ${startValueJson}${endValueJson},"startType":"${startType.name}","type":"${frameType.sql}"}
        |
        |
        |""".stripMargin
    }

  }

  private def toNativeFrame(
      operations: NativePlanBuilder,
      frame: SpecifiedWindowFrame): String = {
    val upper = boundarySql(frame.upper)
    val lower = boundarySql(frame.lower)
    WindowFrame(frame.frameType, lower._1, lower._2, upper._1, upper._2).toString
  }
  private def boundarySql(expr: Expression): (BoundType, Long) = expr match {
    case e: SpecialFrameBoundary =>
      e match {
        case CurrentRow =>
          (BoundType.CurrentRow, 0)
        case UnboundedFollowing =>
          (BoundType.UnboundedFollowing, 0)
        case UnboundedPreceding =>
          (BoundType.UnboundedPreceding, 0)
      }
    case UnaryMinus(n, _) => (BoundType.Preceding, n.sql.toLong.abs)
    case e: Expression =>
      val a = if (e.foldable) {
        e.eval().toString
      } else {
        e.sql
      }
      val result = a.toLong
      if (result < 0) {
        (BoundType.Preceding, a.toLong.abs)
      } else {
        (BoundType.Following, a.toLong)
      }
  }
}
