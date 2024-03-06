package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec}
import org.apache.spark.sql.execution.columnar.expressions.ExpressionConvert
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean = false)
    extends BroadcastHashJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)
    with ColumnarSupport {
  override def supportsColumnar: Boolean =
    true
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
      newRight: SparkPlan): ColumnarBroadcastHashJoinExec =
    new ColumnarBroadcastHashJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      newLeft,
      newRight,
      isNullAwareAntiJoin)

  override def columnarInputRDDs: Seq[(String, RDD[ColumnarBatch])] = {
    val buildSide = getColumnarInputRDD(buildPlan).head
    val streamSide = getColumnarInputRDD(streamedPlan)
    streamSide ++ Seq(
      (
        buildSide._1,
        buildSide._2
          .asInstanceOf[BroadcastBuildSideRDD]
          .copy(numPartitions = streamSide.head._2.getNumPartitions)),
    )

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
    val buildKeysHandle = buildKeys.map(toNativeExpressionJson).toArray
    val streamKeysHandle = streamedKeys.map(toNativeExpressionJson).toArray
    val conditionHandle = if (condition.isEmpty) { null } else {
      toNativeExpressionJson(condition.get)
    }
    operations.join(
      toVeloxJoinType(joinType, buildSide),
      isNullAwareAntiJoin,
      streamKeysHandle,
      buildKeysHandle,
      conditionHandle,
      optionPlan,
      StructType.fromAttributes(filteredColumnarOutput()).catalogString)
    nodeID = operations.nodeId()
  }

}
