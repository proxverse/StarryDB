package org.apache.spark.sql.execution.dict

import org.apache.spark.sql.catalyst.expressions.{
  ExpectsInputTypes,
  Expression,
  NullIntolerant,
  UnaryExpression
}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{
  AbstractDataType,
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType
}
import org.apache.spark.unsafe.types.UTF8String

trait LowCardDictEncoding {

  val child: Expression

  val dict: ColumnDict
}

case class LowCardDictEncode(child: Expression, dict: ColumnDict)
    extends UnaryExpression
    with ExpectsInputTypes
    with NullIntolerant
    with LowCardDictEncoding {

  protected val useByte: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dictRef = ctx.addReferenceObj("dict", dict)
    if (useByte) {
      nullSafeCodeGen(ctx, ev, eval => {
        s"""
           | ${ev.value} = (byte) $dictRef.valueToDictId($eval);
           |""".stripMargin
      })
    } else {
      nullSafeCodeGen(ctx, ev, eval => {
        s"""
           | ${ev.value} = $dictRef.valueToDictId($eval);
           |""".stripMargin
      })
    }
  }

  override protected def nullSafeEval(input: Any): Any =
    if (useByte) {
      dict.valueToDictId(input.asInstanceOf[UTF8String]).byteValue()
    } else {
      dict.valueToDictId(input.asInstanceOf[UTF8String])
    }

  override def dataType: DataType = if (useByte) { ByteType } else { IntegerType }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

}

case class LowCardDictDecode(child: Expression, dict: ColumnDict)
    extends UnaryExpression
    with NullIntolerant
    with LowCardDictEncoding {

  override def nullable: Boolean = true

  override def sql: String = "low_card_dict_decode"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dictRef = ctx.addReferenceObj("dict", dict)
    dict.dataType match {
      case BooleanType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getBoolean($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case ByteType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getByte($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case IntegerType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getInt($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case ShortType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getShort($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case LongType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getLong($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case StringType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getUTF8String($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case BinaryType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getBinary($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case FloatType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getFloat($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case DoubleType =>
        nullSafeCodeGen(ctx, ev, eval => {
          s"""
             | ${ev.value} = $dictRef.vector().getDouble($eval);
             | ${ev.isNull} = $dictRef.vector().isNullAt($eval);
             |""".stripMargin
        })
      case _ => throw new UnsupportedOperationException(s"Unsupported data type ${dict.dataType}")
    }
  }

  override protected def nullSafeEval(input: Any): Any = dict.dataType match {
    case BooleanType =>
      dict.vector().getBoolean(input.asInstanceOf[Int])
    case ByteType =>
      dict.vector().getByte(input.asInstanceOf[Int])
    case IntegerType =>
      dict.vector().getInt(input.asInstanceOf[Int])
    case ShortType =>
      dict.vector().getShort(input.asInstanceOf[Int])
    case LongType =>
      dict.vector().getLong(input.asInstanceOf[Int])
    case StringType =>
      dict.vector().getUTF8String(input.asInstanceOf[Int])
    case BinaryType =>
      dict.vector().getBinary(input.asInstanceOf[Int])
    case FloatType =>
      dict.vector().getFloat(input.asInstanceOf[Int])
    case DoubleType =>
      dict.vector().getDouble(input.asInstanceOf[Int])
    case _ => throw new UnsupportedOperationException(s"Unsupported data type ${dict.dataType}")

  }

  override def dataType: DataType = dict.dataType

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

case class LowCardDictDecodeArray(child: Expression, dict: ColumnDict)
    extends UnaryExpression
    with NullIntolerant
    with LowCardDictEncoding {

  override def nullable: Boolean = true

  override def sql: String = "low_card_dict_decode_array"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(
      ctx,
      ev,
      (arr) => {
        val dictRef = ctx.addReferenceObj("dict", dict)
        val i = ctx.freshName("i")
        val ret = ctx.freshName("ret")
        val (array, decodeDictValue) = dict.dataType match {
          case BooleanType =>
            (
              s"Boolean[] $ret = new Boolean[$arr.numElements()];",
              s"""
                 | $dictRef.vector().getBoolean(${CodeGenerator.getValue(arr, IntegerType, i)});
                 |""".stripMargin)
          case ByteType =>
            (
              s"Byte[] $ret = new Byte[$arr.numElements()];",
              s"""
                 | $dictRef.vector().getByte(${CodeGenerator.getValue(arr, IntegerType, i)});
                 |""".stripMargin)
          case IntegerType =>
            (
              s"Integer[] $ret = new Integer[$arr.numElements()];",
              s"""
                 | $dictRef.vector().getInt(${CodeGenerator.getValue(arr, IntegerType, i)});
                 |""".stripMargin)
          case ShortType =>
            (
              s"Short[] $ret = new Short[$arr.numElements()];",
              s"""
                 | $dictRef.vector().getShort(${CodeGenerator.getValue(arr, IntegerType, i)});
                 |""".stripMargin)
          case LongType =>
            (
              s"Long[] $ret = new Long[$arr.numElements()];",
              s"""
                 | $dictRef.vector().getLong(${CodeGenerator.getValue(arr, IntegerType, i)});
                 |""".stripMargin)
          case StringType =>
            (
              s"UTF8String[] $ret = new UTF8String[$arr.numElements()];",
              s"""
                 | $dictRef.vector().getUTF8String(${CodeGenerator
                   .getValue(arr, IntegerType, i)});
                 |""".stripMargin)
          case BinaryType =>
            (
              s"Byte[][] $ret = new Byte[][$arr.numElements()];",
              s"""
                 | $dictRef.vector().getBinary(${CodeGenerator.getValue(arr, IntegerType, i)});
                 |""".stripMargin)
          case FloatType =>
            (
              s"Float[] $ret = new Float[$arr.numElements()];",
              s"""
                 | $dictRef.vector().getFloat(${CodeGenerator.getValue(arr, IntegerType, i)});
                 |""".stripMargin)
          case DoubleType =>
            (
              s"Double[] $ret = new Double[$arr.numElements()];",
              s"""
                 | $dictRef.vector().getDouble(${CodeGenerator.getValue(arr, IntegerType, i)});
                 |""")
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported data type ${dict.dataType}")
        }
        s"""
         |
         |${array}
         |for (int $i = 0; $i < $arr.numElements(); $i ++) {
         |  if ($arr.isNullAt($i)) {
         |    $ret[$i] = null;
         |  } else {
         |    $ret[$i] = $decodeDictValue;
         |  }
         |}
         |
         | ${ev.value} = new org.apache.spark.sql.catalyst.util.GenericArrayData($ret);
         | """.stripMargin
      })

  override protected def nullSafeEval(input: Any): Any = dict.dataType match {
    case BooleanType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[Boolean](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getBoolean(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case ByteType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[Byte](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getByte(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case IntegerType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[Int](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getInt(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case ShortType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[Short](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getShort(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case LongType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[Long](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getLong(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case StringType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[UTF8String](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getUTF8String(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case BinaryType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[Array[Byte]](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getBinary(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case FloatType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[Float](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getFloat(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case DoubleType =>
      val arr = input.asInstanceOf[ArrayData]
      val decodedArr = new Array[Double](arr.numElements())
      arr.foreach(IntegerType, (i, value) => {
        if (value != null) {
          decodedArr(i) = dict.vector().getDouble(value.asInstanceOf[Int])
        }
      })
      new GenericArrayData(decodedArr)
    case _ => throw new UnsupportedOperationException(s"Unsupported data type ${dict.dataType}")
  }

  override def dataType: DataType = ArrayType(dict.dataType)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

}
