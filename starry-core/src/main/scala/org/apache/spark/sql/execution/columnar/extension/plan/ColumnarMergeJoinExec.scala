package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.expressions.ExpressionConverter
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false)
    extends SortMergeJoinExec(
      leftKeys,
      rightKeys,
      joinType,
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
      .map(a => a.withName(ExpressionConverter.toNativeAttrIdName(a)))
  }
  override def supportsColumnar: Boolean =
    true
  // Disable code generation
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }


  override lazy val extensionMetrics = Map(
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))
  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColumnarMergeJoinExec]
  }

  override def hashCode(): Int = super.hashCode()

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): ColumnarMergeJoinExec =
    new ColumnarMergeJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      condition,
      newLeft,
      newRight,
      isSkewJoin)

  override def collectPartitions(): Seq[FilePartition] = Nil

  override def columnarInputRDDs: Seq[(String, RDD[ColumnarBatch])] = {
    getColumnarInputRDD(left) ++ getColumnarInputRDD(right)
  }

  override def makePlan(operations: NativePlanBuilder): Unit = {
    val optionPlan = right match {
      case c: ColumnarSupport =>
        val builder = new NativePlanBuilder()
        c.makePlan(builder)
        builder.builderAndRelease()
      case _ =>
        throw new UnsupportedOperationException()
    }
    val s2 = left match {
      case c: ColumnarSupport =>
        c.makePlan(operations)
      case other =>
        throw new UnsupportedOperationException()
    }
    val buildKeysHandle = rightKeys.map(toNativeExpressionJson).toArray
    val streamKeysHandle = leftKeys.map(toNativeExpressionJson).toArray
    val conditionHandle = if (condition.isEmpty) { null } else {
      toNativeExpressionJson(condition.get)
    }
    operations.MergeJoin(
      toVeloxJoinType(joinType, BuildRight),
      streamKeysHandle,
      buildKeysHandle,
      conditionHandle,
      optionPlan,
      StructType.fromAttributes(filteredColumnarOutput()).catalogString)
    nodeID = operations.nodeId()
  }

}
