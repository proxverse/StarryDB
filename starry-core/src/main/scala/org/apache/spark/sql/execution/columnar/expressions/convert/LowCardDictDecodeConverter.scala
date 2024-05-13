package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, NativeJsonExpression}
import org.apache.spark.sql.execution.dict.{ColumnDict, LowCardDictEncoding, StartEndDict}
import org.apache.spark.sql.types._

object LowCardDictDecodeConverter extends ExpressionConvertTrait {

  override def lookupFunctionName(expression: Expression): Option[String] =
    Option.apply(expression match {
      case decode: LowCardDictEncoding =>
        val functionName = decode.sql
        decode.dict match {
          case _: StartEndDict =>
            functionName + "_start_end"
          case _: ColumnDict =>
            val suffix: String = getSuffix(decode.dict.dataType)
            functionName + suffix
        }
      case other: Expression =>
        other.prettyName
    })

  override def convert(nativeFunctionName: String, expression: Expression): Expression = {
    val children = expression match {
      case decode: LowCardDictEncoding =>
        val broadcastID = ExpressionConverter.convertToNative(
          new Literal(decode.dict.broadcastID, DataTypes.LongType))
        val broadcastNumBlocks = ExpressionConverter.convertToNative(
          new Literal(decode.dict.broadcastNumBlocks, DataTypes.IntegerType))
        decode.dict match {
          case startEndDict: StartEndDict =>
            val startTransformer = ExpressionConverter.convertToNative(
              new Literal(startEndDict.start, DataTypes.StringType))
            val endTransformer = ExpressionConverter.convertToNative(
              new Literal(startEndDict.end, DataTypes.StringType))
            expression.children ++ Array(
              broadcastID,
              broadcastNumBlocks,
              startTransformer,
              endTransformer)
          case _: ColumnDict =>
            expression.children ++ Array(broadcastID, broadcastNumBlocks)
        }
    }
    convertToNativeCall(
      nativeFunctionName,
      expression.dataType,
      children.map(_.asInstanceOf[NativeJsonExpression]),
      expression
    )
  }

  private def getSuffix(dt: DataType) = {
    val suffix = dt match {
      case StringType =>
        "_varchar"
      case ShortType =>
        "_short"
      case IntegerType =>
        "_integer"
      case LongType =>
        "_long"
      case BooleanType =>
        "_boolean"
      case DoubleType =>
        "_boolean"
      case FloatType =>
        "_float"
      case TimestampType =>
        "_timestamp"
      case ByteType =>
        "_byte"
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported ${dt} type dict")
    }
    suffix
  }

}
