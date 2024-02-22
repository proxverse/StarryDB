package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.columnar.expressions.ExpressionConvert
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ColumnarInputAdapter(child: SparkPlan) extends UnaryExecNode with ColumnarSupport {

  override def supportsColumnar: Boolean =
    true

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def nodeName: String = s"InputAdapter"

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColumnarInputAdapter]
  }

  override def columnarInputRDDs: Seq[(String, RDD[ColumnarBatch])] = getColumnarInputRDD(child)

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarInputAdapter =
    new ColumnarInputAdapter(newChild)

  override def makePlan(operations: NativePlanBuilder): Unit = {
    operations.scan(StructType.fromAttributes(child.output.map(e =>
      e.withName(ExpressionConvert.toNativeAttrIdName(e)))))
    nodeID = operations.nodeId()
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def doExecute(): RDD[InternalRow] = child.execute()
  override def output: Seq[Attribute] = child.output

}
