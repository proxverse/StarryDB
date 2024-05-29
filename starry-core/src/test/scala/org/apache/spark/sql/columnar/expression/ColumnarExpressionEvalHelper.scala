package org.apache.spark.sql.columnar.expression

import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, _}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData, stackTraceToString}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, NativeJsonExpression}
import org.apache.spark.sql.execution.columnar.extension.plan.VeloxRowToColumnConverter
import org.apache.spark.sql.execution.columnar.extension.rule.NativeFunctionPlaceHolder
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.DataTypeTestUtils.integralType
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row, types}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalactic.TripleEqualsSupport.Spread

trait ColumnarExpressionEvalHelper
    extends SparkFunSuite
    with ExpressionEvalHelper
    with ColumnarSharedSparkSession {
  self: SparkFunSuite =>

  System.setProperty(IS_TESTING.key, "true")

  val atomicTypes: Set[DataType] = integralType ++ Set(
    DoubleType,
    FloatType,
    BinaryType,
    BooleanType,
    DateType,
    StringType,
    TimestampType)

  case class GenericInternalSchemaRow(val values: Array[Any], structType: StructType)
      extends BaseGenericInternalRow {

    /** No-arg constructor for serialization. */
    protected def this() = this(null, null)

    def this(size: Int) = this(new Array[Any](size), null)

    override protected def genericGet(ordinal: Int) = values(ordinal)

    override def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = values.clone()

    override def numFields: Int = values.length

    override def setNullAt(i: Int): Unit = { values(i) = null }

    override def update(i: Int, value: Any): Unit = { values(i) = value }
  }

  override protected def create_row(values: Any*): InternalRow = {
    val schema = StructType(
      values
        .map {
          case null =>
            types.NullType
          case v =>
            Literal(v).dataType
        }
        .zipWithIndex
        .map(t => StructField(s"c${t._2}", t._1)))

    GenericInternalSchemaRow(values.map(CatalystTypeConverters.convertToCatalyst).toArray, schema)
  }

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val schema =
      if (inputRow != EmptyRow && inputRow.isInstanceOf[GenericInternalSchemaRow] &&
          !inputRow
            .asInstanceOf[GenericInternalSchemaRow]
            .structType
            .forall(_.dataType.sameType(NullType))) {
        inputRow.asInstanceOf[GenericInternalSchemaRow].structType
      } else {
        StructType(Seq(StructField("a", StringType), StructField("null", StringType)))
      }
    checkEvaluation(expression, expected, inputRow, schema)
  }
  protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow,
      schema: StructType): Unit = {
    val columnBatch = if (inputRow == EmptyRow) {
      createVectorBatch(schema, create_row("foo", null))
    } else {
//      if (inputRow.isInstanceOf[GenericInternalRow]) {
//        inputRow.asInstanceOf[GenericInternalRow].values.map(Literal(_)).zipWithIndex(tp => )
//      }
      createVectorBatch(schema, inputRow)
    }
    val nativeExpression = ExpressionConverter.convertToNative(expression)
    if (!nativeExpression.isInstanceOf[NativeJsonExpression] || nativeExpression.isInstanceOf[NativeFunctionPlaceHolder]) {
      fail("Failed to convert native expression")
    }
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
    try {
      if (!checkResult(head, catalystValue, expression)) {
        fail(
          s"Incorrect evaluation (codegen off): $expression, " +
            s"actual: ${if (expression.dataType.isInstanceOf[ArrayType] && head != null) {
              head.asInstanceOf[ArrayData].array.mkString(",")
            } else {
              head
            }}, " +
            s"expected: $catalystValue")
      }
    } finally {
      columnBatch.close()
      resultBatch.close()
    }
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

  override protected def checkResult(
      result: Any,
      expected: Any,
      exprDataType: DataType,
      exprNullable: Boolean): Boolean = {
    val dataType = UserDefinedType.sqlType(exprDataType)

    // The result is null for a non-nullable expression
    assert(result != null || exprNullable, "exprNullable should be true if result is null")
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Spread[Double @unchecked]) =>
        expected.isWithin(result)
      case (result: InternalRow, expected: InternalRow) =>
        val st = dataType.asInstanceOf[StructType]
        assert(result.numFields == st.length && expected.numFields == st.length)
        st.zipWithIndex.forall {
          case (f, i) =>
            checkResult(
              result.get(i, f.dataType),
              expected.get(i, f.dataType),
              f.dataType,
              f.nullable)
        }
      case (result: ArrayData, expected: ArrayData) =>
        result.numElements == expected.numElements && {
          val ArrayType(et, cn) = dataType.asInstanceOf[ArrayType]
          var isSame = true
          var i = 0
          while (isSame && i < result.numElements) {

            isSame = if (result.isNullAt(i)) { expected.get(i, et) == null } else {
              checkResult(result.get(i, et), expected.get(i, et), et, cn)
            }
            i += 1
          }
          isSame
        }
      case (result: MapData, expected: MapData) =>
        val MapType(kt, vt, vcn) = dataType.asInstanceOf[MapType]
        checkResult(result.keyArray, expected.keyArray, ArrayType(kt, false), false) &&
        checkResult(result.valueArray, expected.valueArray, ArrayType(vt, vcn), false)
      case (result: Double, expected: Double) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Float, expected: Float) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Row, expected: InternalRow) => result.toSeq == expected.toSeq(result.schema)
      case _ =>
        result == expected
    }
  }
}
