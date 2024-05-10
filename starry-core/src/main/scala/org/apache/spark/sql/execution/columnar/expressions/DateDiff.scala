package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant, TimeZoneAwareExpression, TimestampDiff}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, LongType, TimestampType}

import java.time.ZoneId

case class DateDiff(
    unit: String,
    startTimestamp: Expression,
    endTimestamp: Expression,
    timeZoneId: Option[String] = None)
    extends BinaryExpression
    with ImplicitCastInputTypes
    with NullIntolerant
    with TimeZoneAwareExpression {

  def this(unit: String, quantity: Expression, timestamp: Expression) =
    this(unit, quantity, timestamp, None)

  override def left: Expression = startTimestamp
  override def right: Expression = endTimestamp

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)
  override def dataType: DataType = DoubleType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient private lazy val zoneIdInEval: ZoneId = zoneIdForType(endTimestamp.dataType)

  override def nullSafeEval(startMicros: Any, endMicros: Any): Any = {
    DateTimeUtils.timestampDiff(
      unit,
      startMicros.asInstanceOf[Long],
      endMicros.asInstanceOf[Long],
      zoneIdInEval).toDouble
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val zid = ctx.addReferenceObj("zoneId", zoneIdInEval, classOf[ZoneId].getName)
    defineCodeGen(ctx, ev, (s, e) => s"""new Double($dtu.timestampDiff("$unit", $s, $e, $zid))""")
  }

  override def prettyName: String = "date_diff"

  override def sql: String = {
    val childrenSQL = (unit +: children.map(_.sql)).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): DateDiff = {
    copy(startTimestamp = newLeft, endTimestamp = newRight)
  }
}
