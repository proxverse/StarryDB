package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.columnar.ColumnarConf
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

trait NativeExpressionExtensionTrait {

  val SCALAR_NAMESIGS: Seq[NameSig] = Nil

  val SCALAR_SIGS: Seq[Sig] = Nil

}

object PQLExpressionMappings {
  lazy val SCALAR_SIGS: Seq[NameSig] = Seq(
//    NameSig[Add]("plus"),
    NameSig[EqualTo]("eq"),
    NameSig[IsNotNull]("isnotnull"),
    NameSig[IsNull]("isnull"),
    NameSig[EndsWith]("endswith"),
    NameSig[StartsWith]("startswith"),
    NameSig[StringInstr]("instr"),
    NameSig[Length]("length"),
    NameSig[DateDiff]("date_diff"),
//    NameSig[Subtract]("minus"),
    NameSig[Remainder]("mod"),
    NameSig[UnaryMinus]("negate"),
    NameSig[CreateNamedStruct]("row_constructor"),
    NameSig[Hex]("to_hex"),
    NameSig[Unhex]("from_hex"),
    NameSig[Base64]("to_base64"),
    NameSig[UnBase64]("from_base64"),
    NameSig[Levenshtein]("levenshtein_distance"),
  ) ++ NativeExpressionExtension.extensionNameSig
  def expressionsMap: Map[Class[_], String] =
    defaultExpressionsMap

  private lazy val defaultExpressionsMap: Map[Class[_], String] = {
    SCALAR_SIGS
        .map(s => (s.expClass, s.nativeName))
        .toMap[Class[_], String]
  }
}


object ExpressionConvertMapping {

  lazy val SCALAR_SIGS: Seq[Sig] = Seq(
    Sig[StringSplit](SplitConvert),
    Sig[Like](LikeConvert),
    Sig[GetArrayItem](GetArrayItemConvert),
    Sig[SortArray](SortArrayConvert),
    Sig[In](InConvert),
    Sig[InSet](InSetConvert),
    Sig[Concat](ConcatConvert),
    Sig[CreateNamedStruct](CreateNamedStructConvert),
    Sig[Lead](WindowConvert),
    Sig[Lag](WindowConvert),
    Sig[NthValue](WindowConvert),
    Sig[Stack](WindowConvert),
    Sig[Rank](WindowConvert),
    Sig[RowNumber](WindowConvert),
    Sig[DenseRank](WindowConvert),
    Sig[CumeDist](WindowConvert),
    Sig[PercentRank](WindowConvert),
//    Sig[Hex](Hexonvert)
//    Sig[Cast](CastConvert)
  ) ++ NativeExpressionExtension.extensionSig
  val AGGREGATE_SIGS: Seq[Sig] = Seq()
  val WINDOW_SIGS: Seq[Sig] = Seq()
  def expressionsMap: Map[Class[_], ExpressionConvertTrait] =
    defaultExpressionsMap

  private lazy val defaultExpressionsMap: Map[Class[_], ExpressionConvertTrait] = {
    (SCALAR_SIGS ++ AGGREGATE_SIGS ++ WINDOW_SIGS)
      .map(s => (s.expClass, s.convert))
      .toMap[Class[_], ExpressionConvertTrait]
  }
}

case class Sig(expClass: Class[_], convert: ExpressionConvertTrait)

object Sig {
  def apply[T <: Expression: ClassTag](convert: ExpressionConvertTrait): Sig = {
    Sig(scala.reflect.classTag[T].runtimeClass, convert)
  }
}

case class NameSig(expClass: Class[_], nativeName: String)

object NameSig {
  def apply[T <: Expression: ClassTag](convert: String): NameSig = {
    NameSig(scala.reflect.classTag[T].runtimeClass, convert)
  }
}

object NativeExpressionExtension extends Logging {

  lazy val (extensionSig: Seq[Sig], extensionNameSig: Seq[NameSig]) = {
    if (ColumnarConf.expressionExtensionClass.isEmpty) {
      (Nil, Nil)
    } else {
      try {
        val extensionConfClass = Utils.classForName(ColumnarConf.expressionExtensionClass.get)
        val extensionTrait = extensionConfClass
          .getConstructor()
          .newInstance()
          .asInstanceOf[NativeExpressionExtensionTrait]
        (extensionTrait.SCALAR_SIGS, extensionTrait.SCALAR_NAMESIGS)
      } catch {
        // Ignore the error if we cannot find the class or when the class has the wrong type.
        case e @ (_: ClassCastException | _: ClassNotFoundException | _: NoClassDefFoundError) =>
          logWarning(
            s"Cannot create extended expression transformer ${ColumnarConf.expressionExtensionClass.get}",
            e)
      }
    }
  }

}
