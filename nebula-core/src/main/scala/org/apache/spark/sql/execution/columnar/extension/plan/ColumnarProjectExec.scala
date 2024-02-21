package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.columnar.expressions.ExpressionConvert
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarProjectExec(project: Seq[NamedExpression], child: SparkPlan)
    extends ProjectExec(project, child)
    with ColumnarSupport {

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
    other.isInstanceOf[ColumnarProjectExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarProjectExec =
    new ColumnarProjectExec(projectList, newChild)

  override def collectPartitions(): Seq[FilePartition] = {
    child.asInstanceOf[ColumnarSupport].collectPartitions()
  }

  override def makePlanInternal(operations: NativePlanBuilder): Unit = {
    val alias = output.map(ExpressionConvert.toNativeAttrIdName).toArray
    val newProject =
      projectList.map(e => toNativeExpression(e))
    operations.project(alias, newProject.map(_.handle).toArray)

  }

}
