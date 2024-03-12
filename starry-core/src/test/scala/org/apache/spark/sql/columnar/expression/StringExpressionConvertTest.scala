/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.columnar.expression

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, _}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, NativeJsonExpression}
import org.apache.spark.sql.execution.columnar.extension.plan.VeloxRowToColumnConverter
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.Suite

class StringExpressionConvertTest
    extends SparkFunSuite
    with ExpressionEvalHelper
    with ColumnarSharedSparkSession {
  self: Suite =>


  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("LIKE") {
    val schema = StructType(Seq(StructField("a", StringType), StructField("null", StringType)))
    val nullBatch = createVectorBatch(schema, create_row(null))
    val batch = createVectorBatch(schema, create_row("foo", null))

    checkEvaluation('null.string.like(Literal.create("a", StringType)), null, batch)
    checkEvaluation('null.string.like(Literal.create(null, StringType)), null, batch)
    checkEvaluation('a.string.like(Literal.create(null, StringType)), null, batch)
    // simple patterns
    runLike("abdef", "abdef", true)
    runLike("a_%b", "a\\__b", true)
    runLike("addb", "a_%b", true)
    runLike("addb", "a\\__b", false)
    runLike("addb", "a%\\%b", false)
    runLike("a_%b", "a%\\%b", true)
    runLike("addb", "a%", true)
    runLike("addb", "**", false)
    runLike("abc", "a%", true)
    runLike("abc", "b%", false)
    runLike("abc", "bc%", false)
    runLike("a\nb", "a_b", true)
    runLike("ab", "a%b", true)
    runLike("a\nb", "a%b", true)
    // empty input
    checkEvaluation(
      'a.string.like(Literal.create("", StringType)),
      true,
      createVectorBatch(schema, create_row("", null)))

    checkEvaluation(
      'a.string.like(Literal.create("", StringType)),
      false,
      createVectorBatch(schema, create_row("a", null)))

    checkEvaluation(
      'a.string.like(Literal.create("a", StringType)),
      false,
      createVectorBatch(schema, create_row("", null)))

    checkEvaluation(
      Literal.create("a", StringType).like(NonFoldableLiteral.create("a", StringType)),
      true)
    checkEvaluation(
      Literal.create("a", StringType).like(NonFoldableLiteral.create(null, StringType)),
      null)
    checkEvaluation(
      Literal.create(null, StringType).like(NonFoldableLiteral.create("a", StringType)),
      null)
    checkEvaluation(
      Literal.create(null, StringType).like(NonFoldableLiteral.create(null, StringType)),
      null)

    //    // SI-17647 double-escaping backslash
    runLike("""\\\\""", """%\\%""", true)
    runLike("""%%""", """%%""", true)
    runLike("""\__""", """\\\__""", true)
    runLike("""\\\__""", """%\\%\%""", false)
    runLike("""_\\\%""", """%\\""", false)
//
//    // unicode
//    // scalastyle:off nonascii
    runLike("a\u20ACa", "_\u20AC_", true)
    runLike("a€a", "_€_", true)
    runLike("a€a", "_\u20AC_", true)
    runLike("a\u20ACa", "_€_", true)
//    // scalastyle:on nonascii
    runLike("A", "a%", false)
    runLike("a", "A%", false)
    runLike("AaA", "_a_", true)
//
//    // example
    runLike("""%SystemDrive%\Users\John""", """\%SystemDrive\%\\Users%""", true)
  }

  def runLike(row: String, patten: String, answer: Boolean): Unit = {
    val schema = StructType(Seq(StructField("a", StringType), StructField("null", StringType)))
    checkEvaluation(
      'a.string.like(Literal.create(patten, StringType)),
      answer,
      createVectorBatch(schema, create_row(row, null)))

  }

  test("SPLIT") {
    val schema = StructType(Seq(StructField("a", StringType), StructField("b", StringType)))
    val s1 = 'a.string
    val s2 = 'b.string
    val batch1 = createVectorBatch(schema, create_row("aa2bb3cc", "[1-9]+"))
    val batch12 = createVectorBatch(schema, create_row("aa2bb2cc", "2"))
    val batch2 = createVectorBatch(schema, create_row(null, "[1-9]+"))
    val batch3 = createVectorBatch(schema, create_row("aa2bb3cc", null))
    val batch4 = createVectorBatch(schema, create_row("aacbbcddc", null))
//    checkEvaluation(StringSplit('a.string, Literal("[1-9]+"), -1), Seq("aa", "bb", "cc"), batch1)
//    checkEvaluation(
//      StringSplit(Literal("aa2bb3cc"), Literal("[1-9]+"), -1),
//      Seq("aa", "bb", "cc"),
//      batch1)
//    checkEvaluation(
//      StringSplit(Literal("aa2bb3cc"), Literal("[1-9]+"), 2),
//      Seq("aa", "bb3cc"),
//      batch1)
    // limit = 0 should behave just like limit = -1
    checkEvaluation(StringSplit('a.string, Literal("c"), 0), Seq("aa", "bb", "dd", ""), batch4)
    checkEvaluation(StringSplit('a.string, Literal("c"), -1), Seq("aa", "bb", "dd", ""), batch4)
    checkEvaluation(StringSplit(s1, s2, -1), Seq("aa", "bb", "cc"), batch12)
//    checkEvaluation(StringSplit(s1, s2, -1), null, batch2)
//    checkEvaluation(StringSplit(s1, s2, -1), null, batch2)
  }

  def createVectorBatch(schema: StructType, row: InternalRow): ColumnarBatch = {
    val batch = ColumnBatchUtils.createWriterableColumnBatch(1, schema)
    batch.asInstanceOf[VeloxColumnarBatch].setSchema(schema)
    new VeloxRowToColumnConverter(schema)
      .convert(
        row,
        batch
          .asInstanceOf[VeloxColumnarBatch]
          .getColumns
          .map(_.asInstanceOf[WritableColumnVector]))
    batch.setNumRows(1)
    batch
  }

  protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      columnBatch: ColumnarBatch): Unit = {
    val nativeExpression = ExpressionConverter.convertToNative(expression)
    if (!nativeExpression.isInstanceOf[NativeJsonExpression]) {
      fail("Failed to convert native expression")
    }
    columnBatch.asInstanceOf[VeloxColumnarBatch]
    val resultBatch = NativeExpressionConvert.evalWithBatch(
      nativeExpression.asInstanceOf[NativeJsonExpression].native,
      columnBatch.asInstanceOf[VeloxColumnarBatch],
      StructType(Seq(StructField("result", expression.dataType))))
    val catalystValue = CatalystTypeConverters.convertToCatalyst(expected)
    val row = resultBatch.rowIterator().next()
    val head = if (row.isNullAt(0)) {
      null
    } else {
      row.get(0, expression.dataType)
    }
    //    val scalaValue = CatalystTypeConverters.convertToScala(
//      resultBatch.rowIterator().next().toSeq(structType).head,
//      expression.dataType)
    if (!checkResult(head, catalystValue, expression)) {
      fail(
        s"Incorrect evaluation (codegen off): $expression, " +
          s"actual: $head, " +
          s"expected: $catalystValue")
    }
  }

}
