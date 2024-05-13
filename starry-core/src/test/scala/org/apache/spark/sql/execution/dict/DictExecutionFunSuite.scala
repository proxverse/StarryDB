package org.apache.spark.sql.execution.dict

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.StarryContext
import org.apache.spark.sql.execution.columnar.VeloxConstantsVector
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, NativeJsonExpression}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.Assertions

class DictExecutionFunSuite extends ColumnarSharedSparkSession {

  def replaceAttrToDictAttr(expression: Expression): Expression = {
    expression.transform {
      case _: UnresolvedAttribute =>
        AttributeReference("dict", StringType, nullable = true)(
          NamedExpression.newExprId,
          Seq.empty[String])
    }
  }

  private def fetchDictVector(execDict: ExecutionColumnDict): ColumnVector = {
    ExecutorDictManager.fetchDictVector(execDict.broadcastID, execDict.broadcastNumBlocks)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    StarryContext.startNewContext()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    StarryContext.clean()
  }

  test("testBooleanExpression") {
    val dict = new SimpleColumnDict(Array("A", "B", "C"))
    val boundExpr = replaceAttrToDictAttr(col("dict").isin(Array("A"): _*).expr)
    val expression = ExpressionConverter.convertToNative(boundExpr).asInstanceOf[NativeJsonExpression]
    val vector = fetchDictVector(new ExecutionColumnDict(dict, boundExpr, BooleanType, expression.native))
    Assertions.assert(vector.getBoolean(0))
    Assertions.assert(!vector.getBoolean(1))
    Assertions.assert(!vector.getBoolean(2))
    ExecutorDictManager.executionDictCache.invalidateAll()
  }

  test("testStringExpression") {
    val dict = new SimpleColumnDict(Array("A", "B", "C"))
    val boundExpr = replaceAttrToDictAttr(
      when(col("dict").isin(Array("A"): _*), col("dict")).otherwise(lit("other")).expr)
    val expression = ExpressionConverter.convertToNative(boundExpr).asInstanceOf[NativeJsonExpression]
    val vector = fetchDictVector(new ExecutionColumnDict(dict, boundExpr, StringType, expression.native))
    Assertions.assert(vector.getUTF8String(0).equals(UTF8String.fromString("A")))
    Assertions.assert(vector.getUTF8String(1).equals(UTF8String.fromString("other")))
    Assertions.assert(vector.getUTF8String(2).equals(UTF8String.fromString("other")))
    ExecutorDictManager.executionDictCache.invalidateAll()
    ExecutorDictManager.dictCache.invalidateAll()
  }

  test("testConstantExpression") {
    val dict = new SimpleColumnDict(Array("A", "B", "C"))
    val boundExpr = replaceAttrToDictAttr(
      when(col("dict").isin(Array("D"): _*), col("dict")).otherwise(lit("other")).expr)
    val expression = ExpressionConverter.convertToNative(boundExpr).asInstanceOf[NativeJsonExpression]
    val vector = fetchDictVector(new ExecutionColumnDict(dict, boundExpr, StringType, expression.native))
    Assertions.assert(vector.isInstanceOf[VeloxConstantsVector])
    Assertions.assert(vector.getUTF8String(0).equals(UTF8String.fromString("other")))
    Assertions.assert(vector.getUTF8String(1).equals(UTF8String.fromString("other")))
    Assertions.assert(vector.getUTF8String(2).equals(UTF8String.fromString("other")))
    ExecutorDictManager.executionDictCache.invalidateAll()
    ExecutorDictManager.dictCache.invalidateAll()
  }

}
