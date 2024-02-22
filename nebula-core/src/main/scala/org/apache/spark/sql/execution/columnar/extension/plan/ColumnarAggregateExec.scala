package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  Final,
  Partial,
  PartialMerge
}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  NamedExpression,
  aggregate
}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec}
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConvert, NativeExpression}
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarAggregateExec {
  val STEP_PARTIAL = "PARTIAL"
  val STEP_FINAL = "FINAL"
  val STEP_INTERMEDIATE = "INTERMEDIATE"
  val STEP_SINGLE = "SINGLE"

  private val nativeAggFuncMapping =
    Map[String, String](
      "collect_list" -> "array_agg",
      "collect_set" -> "set_agg",
      "first" -> "first_ignore_null")

  private def toNativeAggStep(aggregateExpression: AggregateExpression): String = {
    aggregateExpression.mode match {
      case aggregate.Partial => STEP_PARTIAL
      case aggregate.PartialMerge => STEP_INTERMEDIATE
      case aggregate.Final => STEP_FINAL
      case aggregate.Complete => STEP_SINGLE
    }
  }

  private def toNativeAggFuncName(originName: String): String = {
    if (nativeAggFuncMapping.contains(originName)) {
      nativeAggFuncMapping.apply(originName)
    } else {
      originName
    }
  }

  private def toNativeAggExprNode(
      operations: NativePlanBuilder,
      aggExpr: AggregateExpression,
      inputs: Array[String],
      rawInputs: Array[String],
      useMergeFuncHack: Boolean): String = {
    val mask = if (aggExpr.filter.isDefined) {
      ExpressionConvert.convertToNativeJson(aggExpr.filter.get, true)
    } else {
      null
    }
    val funcName = toNativeAggFuncName(aggExpr.aggregateFunction.prettyName)
    val step = toNativeAggStep(aggExpr)
    operations.buildAggregate(
      funcName,
      inputs,
      rawInputs,
      step,
      mask,
      null,
      null,
      false,
      step == STEP_INTERMEDIATE && useMergeFuncHack)
  }

  private def toIntermediateType(
      aggExpr: AggregateExpression,
      operations: NativePlanBuilder = null): DataType = {
    val children = aggExpr.aggregateFunction.children
      .map(ExpressionConvert.convertToNative(_, true).asInstanceOf[NativeExpression].handle)
      .toArray
    if (operations == null) {
      // TODO move resolveAggType outside NativePlanBuilder
      val builder = new NativePlanBuilder()
      val ret = builder.resolveAggType(
        toNativeAggFuncName(aggExpr.aggregateFunction.prettyName),
        children,
        STEP_PARTIAL)
      builder.close()
      ret
    } else {
      val ret = operations.resolveAggType(
        toNativeAggFuncName(aggExpr.aggregateFunction.prettyName),
        children,
        STEP_PARTIAL)
      ret
    }
  }

  def apply(hashAggregateExec: HashAggregateExec): ColumnarAggregateExec = {
    new ColumnarAggregateExec(
      hashAggregateExec.requiredChildDistributionExpressions,
      hashAggregateExec.isStreaming,
      hashAggregateExec.numShufflePartitions,
      hashAggregateExec.groupingExpressions,
      hashAggregateExec.aggregateExpressions,
      hashAggregateExec.aggregateAttributes,
      hashAggregateExec.child)
  }
}

case class ColumnarAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    isStreaming: Boolean,
    numShufflePartitions: Option[Int],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    child: SparkPlan)
    extends BaseAggregateExec
    with ColumnarSupport {

  import ColumnarAggregateExec._

  lazy private val step = aggregateExpressions.headOption
    .map(toNativeAggStep)
    .getOrElse(STEP_SINGLE)
  lazy private val needMergeHack = aggregateExpressions
    .exists(_.isDistinct) && step == STEP_INTERMEDIATE

  override def supportsColumnar: Boolean = true
  // Disable code generation

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColumnarAggregateExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarAggregateExec =
    copy(child = newChild)

  override def makePlanInternal(operations: NativePlanBuilder): Unit = {
    val aggNodes = aggregateExpressions.map { aggExpr =>
      val rawInputs = aggExpr.aggregateFunction.children
        .map(ExpressionConvert.convertToNativeJson(_, true))
        .toArray
      val inputs = toNativeAggInput(aggExpr)
        .map(ExpressionConvert.convertToNativeJson(_, true))
        .toArray
      toNativeAggExprNode(
        operations,
        aggExpr,
        inputs,
        rawInputs,
        needMergeHack && !aggExpr.isDistinct)
    }.toArray
    val aggNames = aggregateExpressions.map(aggExpr =>
      ExpressionConvert.toNativeAttrIdName(toNativeAggOutput(aggExpr)))
    val group = groupingExpressions
      .map(ExpressionConvert.convertToNative(_, true).asInstanceOf[NativeExpression].handle)
      .toArray

    operations.aggregate(
      if (needMergeHack) STEP_PARTIAL else step,
      group,
      aggNames.toArray,
      aggNodes,
      false)
  }

  // Hack the native agg func input/output references
  // As spark agg func on different stage are using the same result id. We can
  // use result id to link agg func on different stage.
  //               input                                  output
  // PARTIAL:       raw                            result_attr(intermediate_type)
  // INTERMEDIATE:  result_attr(intermediate_type) result_attr(intermediate_type)
  // FINAL:         result_attr(intermediate_type) result_attr(result_type)
  // SINGLE:        raw                            result_attr(result_type)
  //
  // Note that distinct function is created with new result id in AggUtils,
  // We need to set result id back for distinct functions.
  //
  private def normalizeResultAttrName(aggExpr: AggregateExpression): Attribute = {
    aggExpr.resultAttribute.withName(aggExpr.aggregateFunction.prettyName)
  }

  private def toNativeAggInput(aggExpr: AggregateExpression): Seq[Expression] = {
    aggExpr.mode match {
      case Final if aggExpr.isDistinct =>
        val aggAttr = aggregateAttributes(aggregateExpressions.indexOf(aggExpr))
        normalizeResultAttrName(aggExpr)
          .withDataType(toIntermediateType(aggExpr)).withExprId(aggAttr.exprId) :: Nil
      case PartialMerge | Final =>
        normalizeResultAttrName(aggExpr).withDataType(toIntermediateType(aggExpr)) :: Nil
      case _ =>
        aggExpr.aggregateFunction.children
    }
  }

  private def toNativeAggOutput(aggExpr: AggregateExpression): Attribute = {
    aggExpr.mode match {
      case Partial if aggExpr.isDistinct =>
        val aggAttr = aggregateAttributes(aggregateExpressions.indexOf(aggExpr))
        normalizeResultAttrName(aggExpr)
          .withDataType(toIntermediateType(aggExpr))
          .withExprId(aggAttr.exprId)
      case Final if aggExpr.isDistinct =>
        aggregateAttributes(aggregateExpressions.indexOf(aggExpr))
      case PartialMerge | Partial =>
        normalizeResultAttrName(aggExpr).withDataType(toIntermediateType(aggExpr))
      case _ =>
        aggExpr.resultAttribute
    }
  }

  override def output: Seq[Attribute] = {
    groupingExpressions.map(_.toAttribute) ++ aggregateExpressions.map(toNativeAggOutput)
  }

  override def canEqual(other: Any): Boolean = {
    other.isInstanceOf[ColumnarAggregateExec]
  }
  override def initialInputBufferOffset: Int = 0 // unused

  override def resultExpressions: Seq[NamedExpression] = output // unused

}
