package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.sql.catalyst.expressions.{Attribute, Generator, NamedExpression}
import org.apache.spark.sql.execution.{GenerateExec, ProjectExec, SparkPlan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.columnar.expressions.ExpressionConvert
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.execution.datasources.FilePartition

class ColumnarGenerateExec(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan)
    extends GenerateExec(generator, requiredChildOutput, outer, generatorOutput, child)
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
    other.isInstanceOf[ColumnarGenerateExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarGenerateExec =
    new ColumnarGenerateExec(generator, requiredChildOutput, outer, generatorOutput, newChild)

  override def makePlanInternal(operations: NativePlanBuilder): Unit = {
    val unnestVariables =
      generator.children.map(ExpressionConvert.convertToNativeJson(_, true)).toArray
    val replicateVariables =
      requiredChildOutput.map(ExpressionConvert.convertToNativeJson(_, true)).toArray
    val unnestNames = generatorOutput.map(ExpressionConvert.toNativeAttrIdName).toArray
    val ordinalityName = Array.empty[String]
    operations.unnest(replicateVariables, unnestVariables, unnestNames, null)

  }

}
