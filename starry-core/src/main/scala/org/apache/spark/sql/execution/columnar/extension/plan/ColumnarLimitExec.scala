package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.sql.execution.{BaseLimitExec, SparkPlan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution}
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.execution.datasources.FilePartition

case class ColumnarLimitExec(limit: Int, isPartial: Boolean, child: SparkPlan)
    extends BaseLimitExec
    with ColumnarSupport {

  override def supportsColumnar: Boolean = true
  // Disable code generation
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override def requiredChildDistribution: List[Distribution] =
    if (!isPartial) { AllTuples :: Nil } else { super.requiredChildDistribution.toList }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColumnarLimitExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarLimitExec =
    new ColumnarLimitExec(limit, isPartial, newChild)

  override def collectPartitions(): Seq[FilePartition] = {
    child.asInstanceOf[ColumnarSupport].collectPartitions()
  }

  override def makePlanInternal(operations: NativePlanBuilder): Unit = {
    operations.limit(0, limit)
  }

}
