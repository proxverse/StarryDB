package org.apache.spark.sql.execution.dict

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreePattern}
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, NativeJsonExpression}
import org.apache.spark.sql.internal.StarryConf
import org.apache.spark.sql.types._

object RewriteExpressionWithGlobalDict extends Logging {

  import RewriteContext._

  def rewriteExpr(expression: Expression,
                  useExecution: Boolean = false): Expression = {
    expression match {
      // ----- AGG EXPRS ----- //
      case aggWithAlias@Alias(_: AggregateExpression, _) =>
        rewriteAggregateExpression(aggWithAlias)
      // ----- WINDOW EXPRS ----- //
      case alias@Alias(_: WindowExpression, _) =>
        rewriteWindowExpression(alias)
      // ----- NO AGG EXPRS ----- //
      // direct references
      case _: AttributeReference | Alias(_: AttributeReference, _: String) =>
        expression.transformToEncodedRef()
      // skip encoded col
      case a @ Alias(_: LowCardDictEncoding, _: String) =>
        a
      // custom
      case custom if DictExpressionRewriteRegistry.findNonAggExprRewrite(custom).isDefined =>
        DictExpressionRewriteRegistry.findNonAggExprRewrite(custom).get.rewrite(custom)
      // decode for other func call
      case e =>
        tryDecodeDown(e, useExecution)
    }
  }

  def tryDecodeDown(expr: Expression, useExecution: Boolean = false): Expression = {
    val newExpr = expr match {
      // reference that is not inside a computable func call needs decode
      case ar: AttributeReference =>
        ar.decodeInChildren()
      // keep window related exprs
      case wexpr: WindowExpression =>
        wexpr
      case wspec: WindowSpecDefinition =>
        wspec
      case wframe: SpecifiedWindowFrame =>
        wframe
      // is not null/is null does not require decode
      case IsNotNull(_: AttributeReference) | IsNull(_: AttributeReference)
        if expr.children.head.encodedRefInChildren().isDefined =>
        expr.withNewChildren(Seq(expr.children.head.encodedRefInChildren().get))
      // handle bool typed
      case expr if useExecution && (expr.dataType == StringType || expr.dataType == BooleanType)
          && !expr.containsPattern(TreePattern.AGGREGATE_EXPRESSION) =>
        tryDictExecution(expr)
      // others
      case e =>
        e
    }

    if (newExpr fastEquals expr) {
      CurrentOrigin.withOrigin(expr.origin) {
        expr.mapChildren(tryDecodeDown(_, useExecution))
      }
    } else {
      CurrentOrigin.withOrigin(expr.origin) {
        newExpr
      }
    }
  }

  private def rewriteAggregateExpression(expression: Expression): Expression = {
    expression match {
      // custom rewrite
      case customExpr if DictExpressionRewriteRegistry.findAggExprRewrite(customExpr).isDefined =>
        DictExpressionRewriteRegistry.findAggExprRewrite(customExpr).get.rewrite(customExpr)
      // try decode otherwise
      case e =>
        tryDecodeDown(e)
    }
  }

  private def rewriteWindowExpression(expression: Expression): Expression = {
    val customRewrite = DictExpressionRewriteRegistry.findWindowExprRewrite(expression)
    if (customRewrite.isDefined) {
      customRewrite.get.rewrite(expression)
    } else {
      tryDecodeDown(expression)
    }
  }

  // TODO refactor this
  private def tryDictExecution(expression: Expression): Expression = {
    val canDoDictExecution = (expr: Expression) => {
      lazy val refDict = expr.references.head.dictInChildren()
      lazy val validRefDict = refDict.isDefined && refDict.get.supportExecution
      StarryConf.dictExecutionEnabled && expr.references.size == 1 && validRefDict
    }
    expression match {
      case namedExpr@Alias(expr, name) if canDoDictExecution(expr) =>
        try {
          val encodedRef = expr.references.head.encodedRefInChildren().get
          val boundExpr = expr.transform {
            case _: AttributeReference =>
              AttributeReference("dict", StringType, nullable = true)(
                NamedExpression.newExprId,
                Seq.empty[String])
          }
          val expression = ExpressionConverter.convertToNative(boundExpr)
          expression match {
            case native: NativeJsonExpression =>
              val transformedExpr = Alias(encodedRef, s"${name}_enc_transformed")()
              val execDict = ExecutionColumnDict(encodedRef.dict.get, boundExpr, expr.dataType, native.native)
              namedExpr.recordMapping(transformedExpr, execDict)
              transformedExpr
            case _ =>
              namedExpr
          }
        } catch {
          case e =>
            log.info(s"Compile dict sql error, skip dict execution ", e)
            namedExpr
        }

      case expr: Expression if canDoDictExecution(expr) =>
        val encodedRef = expr.references.head.encodedRefInChildren().get
        val boundExpr = expr.transform {
          case _: AttributeReference =>
            AttributeReference("dict", StringType, nullable = true)(
              NamedExpression.newExprId,
              Seq.empty[String])
        }
        try {
          val expression = ExpressionConverter.convertToNative(boundExpr)
          expression match {
            case native: NativeJsonExpression =>
              val execDict = ExecutionColumnDict(encodedRef.dict.get, boundExpr, expr.dataType, native.native)
              LowCardDictDecode(encodedRef, execDict)
            case _ =>
              expr
          }
        } catch {
          case e =>
            log.info(s"Compile dict sql error, skip dict execution ", e)
            expr
        }
      case e =>
        e
    }
  }

}