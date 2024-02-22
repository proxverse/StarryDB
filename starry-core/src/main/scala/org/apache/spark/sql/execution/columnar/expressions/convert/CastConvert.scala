package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

object CastConvert extends ExpressionConvertTrait {

  override def beforeConvert(expression: Expression): Expression = {
    val cast = expression.asInstanceOf[Cast]
    // Common whitespace to be trimmed, including: ' ', '\n', '\r', '\f', etc.
    val trimWhitespaceStr = " \t\n\u000B\u000C\u000D\u001C\u001D\u001E\u001F"
    // Space separator.
    val trimSpaceSepStr = "\u1680\u2008\u2009\u200A\u205F\u3000" +
        ('\u2000' to '\u2006').toList.mkString
    // Line separator.
    val trimLineSepStr = "\u2028"
    // Paragraph separator.
    val trimParaSepStr = "\u2029"
    // Needs to be trimmed for casting to float/double/decimal
    val trimSpaceStr = ('\u0000' to '\u0020').toList.mkString
    cast.dataType match {
      case BinaryType | _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] =>
        cast
      case FloatType | DoubleType | _: DecimalType =>
        cast.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(cast.child, Some(Literal(trimSpaceStr)))
            cast.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            cast
        }
      case _ =>
        cast.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(cast.child, Some(Literal(trimWhitespaceStr +
                trimSpaceSepStr + trimLineSepStr + trimParaSepStr)))
            cast.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            cast
        }
    }
  }


}
