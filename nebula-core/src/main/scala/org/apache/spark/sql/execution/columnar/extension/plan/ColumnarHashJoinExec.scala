package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConvert, NativeExpression}
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.types.StructType

class ColumnarHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false)
    extends ShuffledHashJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)
    with ColumnarSupport {

  override def output: Seq[Attribute] =
    super.output.filterNot(
      a =>
        a.name.startsWith(ColumnarSupport.JOIN_LEFT_PREFIX) || a.name.startsWith(
          ColumnarSupport.JOIN_RIGHT_PREFIX))

  def filteredColumnarOutput(): Seq[Attribute] = {
    output
      .filterNot(
        a =>
          a.name.startsWith(ColumnarSupport.JOIN_LEFT_PREFIX) || a.name.startsWith(
            ColumnarSupport.JOIN_RIGHT_PREFIX))
      .map(a => a.withName(ExpressionConvert.toNativeAttrIdName(a)))
  }
  override def supportsColumnar: Boolean =
    true
  // Disable code generation
  override def supportCodegen: Boolean = false

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
    other.isInstanceOf[ColumnarHashJoinExec]
  }

  override def hashCode(): Int = super.hashCode()

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): ColumnarHashJoinExec =
    new ColumnarHashJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      newLeft,
      newRight,
      isSkewJoin)

  override def collectPartitions(): Seq[FilePartition] = Nil

  override def columnarInputRDDs: Seq[(String, RDD[ColumnarBatch])] = {
    getColumnarInputRDD(streamedPlan) ++ getColumnarInputRDD(buildPlan)
  }

  override def makePlan(operations: NativePlanBuilder): Unit = {
    val optionPlan = buildPlan match {
      case c: ColumnarSupport =>
        val builder = new NativePlanBuilder()
        c.makePlan(builder)
        builder.builderAndRelease()
      case _ =>
        throw new UnsupportedOperationException()
    }
    val s2 = streamedPlan match {
      case c: ColumnarSupport =>
        c.makePlan(operations)
      case other =>
        throw new UnsupportedOperationException()
    }
    val buildKeysHandle = buildKeys
      .map(e => toNativeExpression(e).handle)
      .toArray
    val streamKeysHandle = streamedKeys
      .map(e => toNativeExpression(e).handle)
      .toArray
    val conditionHandle = if (condition.isEmpty) { 0 } else {
      toNativeExpression(condition.get).handle
    }

    operations.join(
      toVeloxJoinType(joinType, buildSide),
      false,
      streamKeysHandle,
      buildKeysHandle,
      conditionHandle,
      optionPlan,
      StructType.fromAttributes(filteredColumnarOutput()).catalogString)
    nodeID = operations.nodeId()
  }

}
