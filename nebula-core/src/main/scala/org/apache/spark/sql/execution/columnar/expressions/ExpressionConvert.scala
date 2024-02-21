package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.columnar.expressions.convert.ExpressionConvertMapping
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.implicitCast
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.columnar.VeloxWritableColumnVector
import org.apache.spark.sql.execution.columnar.extension.plan.RowToColumnConverter
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
import org.apache.spark.unsafe.types.UTF8String

import java.util.regex.Pattern

object ExpressionConvert extends Logging {

  val nullTypeTransform: PartialFunction[Expression, Expression] = {
    case cast: Cast if cast.child.dataType.sameType(NullType) =>
      Literal.create(null, cast.dataType)

    case arrayRepeat: ArrayRepeat if arrayRepeat.left.dataType.sameType(NullType) =>
      ArrayRepeat(Literal.create(null, StringType), arrayRepeat.right)
    case arrayDistinct: ArrayDistinct
        if arrayDistinct.child.dataType.asInstanceOf[ArrayType].elementType.sameType(NullType) =>
      ArrayDistinct(Literal.create(Seq(null, null, null), ArrayType(StringType)))
    case murmur3Hash: Murmur3Hash if murmur3Hash.children.exists(_.dataType.sameType(NullType)) =>
      val dataType = murmur3Hash.children.filterNot(_.dataType.sameType(NullType)).head.dataType
      val newChildren = murmur3Hash.children.map { e =>
        if (e.dataType.sameType(NullType)) {
          Literal.create(null, dataType)
        } else {
          e
        }
      }
      murmur3Hash.withNewChildren(newChildren)

    case e: ImplicitCastInputTypes if e.inputTypes.nonEmpty =>
      val children: Seq[Expression] = e.children.zip(e.inputTypes).map {
        case (in, expected) =>
          // If we cannot do the implicit cast, just use the original input.
          implicitCast(in, expected).getOrElse(in)
      }
      e.withNewChildren(children)

    case e: ExpectsInputTypes if e.inputTypes.nonEmpty =>
      // Convert NullType into some specific target type for ExpectsInputTypes that don't do
      // general implicit casting.
      val children: Seq[Expression] = e.children.zip(e.inputTypes).map {
        case (in, expected) =>
          if (in.dataType == NullType && !expected.acceptsType(NullType)) {
            Literal.create(null, expected.defaultConcreteType)
          } else {
            in
          }
      }
      e.withNewChildren(children)
    case other => other
  }

  def convertToNativeJson(expression: Expression, useAlias: Boolean = false): String = {
    val expression1 = convertToNative(expression, useAlias).asInstanceOf[NativeExpression]
    val json = NativeExpressionConvert.nativeSerializeExpr(expression1.handle)
    releaseHandle(expression1)
    json
  }

  def convertToNative(expression: Expression, useAlias: Boolean = false): Expression = {
    beforeProcess(expression.transformUp(nullTypeTransform)).transformUp {
      case attr: AttributeReference =>
        val attrName = if (useAlias) {
          toNativeAttrIdName(attr)
        } else {
          attr.name
        }
        val handle = NativeExpressionConvert.nativeCreateFieldAccessTypedExprHanlde(
          attrName,
          attr.dataType.catalogString)
        NativeExpression(handle, attr)
      case literal @ Literal(v, dt) =>
        literal.dataType match {
          case nulltypt: NullType =>
            literal
          case floatType: FloatType if literal.value != null && literal.value.equals(Float.NaN) =>
            val l = NativeExpressionConvert.nativeCreateCallTypedExprHanlde(
              "float_nan",
              dt.catalogString,
              Array.empty)
            NativeExpression(l, literal)
          case _: DoubleType if literal.value != null && literal.value.equals(Double.NaN) =>
            val l = NativeExpressionConvert.nativeCreateCallTypedExprHanlde(
              "nan",
              dt.catalogString,
              Array.empty)
            NativeExpression(l, literal)
          case other =>
            val row = InternalRow.fromSeq(Seq(v))
            val converter = RowToColumnConverter.getConverterForType(dt, true)
            val vector = VeloxWritableColumnVector.createVector(1, literal.dataType)
            converter.append(row, 0, vector)
            val handle = NativeExpressionConvert.nativeCreateConstantTypedExprHanlde(
              literal.dataType.catalogString,
              vector.getNative)
            vector.close()
            NativeExpression(handle, literal)
        }

      case alias: Alias =>
        alias.child
      case other =>
        functionCall(other)
    }
  }

  def beforeProcess(expression: Expression): Expression = expression.transformUp {
    case o if ExpressionConvertMapping.expressionsMap.contains(o.getClass) =>
      ExpressionConvertMapping.expressionsMap.apply(o.getClass).beforeConvert(o)

    case scalarSubquery: org.apache.spark.sql.execution.ScalarSubquery =>
      try {
        new Literal(scalarSubquery.eval(), scalarSubquery.dataType)
      } catch {
        case e: IllegalArgumentException =>
          scalarSubquery.updateResult()
          new Literal(scalarSubquery.eval(), scalarSubquery.dataType)
      }
    case other => other
  }

  def writerToVector(
      dataType: DataType,
      v: Any,
      vector: WritableColumnVector,
      rowId: Int): Unit = {
    dataType match {
      case _ if v == null => vector.appendNull()
      case BooleanType => vector.appendBoolean(v.asInstanceOf[Boolean])
      case ByteType => vector.appendByte(v.asInstanceOf[Byte])
      case ShortType => vector.appendShort(v.asInstanceOf[Short])
      case IntegerType | DateType | _: YearMonthIntervalType =>
        vector.appendInt(v.asInstanceOf[Int])
      case LongType | TimestampType | _: DayTimeIntervalType =>
        vector.appendLong(v.asInstanceOf[Long])
      case FloatType =>
        vector.appendFloat(v.asInstanceOf[Float])
      case DoubleType => vector.appendDouble(v.asInstanceOf[Double])
      //          case _: DecimalType => v.isInstanceOf[Decimal]
      //          case CalendarIntervalType => v.isInstanceOf[CalendarInterval]
      case BinaryType =>
        vector.reserveAdditional(1)
        vector.putByteArray(rowId, v.asInstanceOf[Array[Byte]])
        vector.addElementsAppended(1)
      case StringType =>
        vector.reserveAdditional(1)
        vector.putByteArray(rowId, v.asInstanceOf[UTF8String].getBytes)
        vector.addElementsAppended(1)
      //          case st: StructType =>
      //            v.isInstanceOf[InternalRow] && {
      //              val row = v.asInstanceOf[InternalRow]
      //              st.fields.map(_.dataType).zipWithIndex.forall {
      //                case (dt, i) => doValidate(row.get(i, dt), dt)
      //              }
      //            }
      case at: ArrayType =>
        val array = v.asInstanceOf[ArrayData].array
        array.zipWithIndex
          .foreach(t => writerToVector(at.elementType, t._1, vector.arrayData(), t._2))
        vector.putArray(0, 0, array.length)
      //      case mt: MapType =>
      //        v.isInstanceOf[MapData] && {
      //          val map = v.asInstanceOf[MapData]
      //          doValidate(map.keyArray(), ArrayType(mt.keyType)) &&
      //              doValidate(map.valueArray(), ArrayType(mt.valueType))
      //        }
      //      case ObjectType(cls) => cls.isInstance(v)
      //      case udt: UserDefinedType[_] => doValidate(v, udt.sqlType)
      case _ => throw new UnsupportedOperationException(s"Unsupported type ${dataType}")
    }
  }

  def functionCall(expression: Expression): Expression = {
    val nativeFunctionName = ExpressionNamingProcess.lookupFunctionName(expression)
    val functionName = if (nativeFunctionName.isEmpty) {
      expression.prettyName
    } else {
      nativeFunctionName.get
    }
    expression match {
      case o if ExpressionConvertMapping.expressionsMap.contains(o.getClass) =>
        ExpressionConvertMapping.expressionsMap
          .apply(o.getClass)
          .convert(nativeFunctionName.get, o)
      case other =>
        if (!expression.children.forall(_.isInstanceOf[NativeExpression])) {
          logInfo(s"children has normal expression, skip transform ${expression}")
          return expression
        }
        try {
          NativeExpression(
            NativeExpressionConvert
              .nativeCreateCallTypedExprHanlde(
                functionName,
                expression.dataType.catalogString,
                other.children.map(_.asInstanceOf[NativeExpression].handle).toArray),
            expression.withNewChildren(
              expression.children.map(_.asInstanceOf[NativeExpression]).map(_.transformed)))
        } catch {
          case e =>
            logInfo("Error for convert expression", e)
            expression
        }

    }

  }

  def releaseHandle(expression: Expression): Unit = {
    expression.foreach {
      case nativeExpression: NativeExpression =>
        NativeExpressionConvert.nativeReleaseHandle(nativeExpression.handle)
      case _ =>
    }
  }

  private val NON_ALPHANUMERIC_PATTERN = Pattern.compile("[^a-zA-Z0-9]")

  def toNativeAttrIdName(a: Attribute): String = {
    NON_ALPHANUMERIC_PATTERN.matcher(s"${a.name}_${a.exprId.id}").replaceAll("_")
  }

  def convertToNativeAggExpression(
      expression: AggregateExpression,
      useAlias: Boolean = true): NativeExpression = {
    val newChildren = expression.aggregateFunction.children.map(e => convertToNative(e, useAlias))
    NativeExpression(
      NativeExpressionConvert
        .nativeCreateCallTypedExprHanlde(
          expression.aggregateFunction.prettyName,
          expression.dataType.catalogString,
          newChildren.map(_.asInstanceOf[NativeExpression].handle).toArray),
      expression.withNewChildren(
        expression.children.map(_.asInstanceOf[NativeExpression]).map(_.transformed)))
  }

}
