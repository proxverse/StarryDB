package org.apache.spark.sql.execution.dict

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreePattern}
import org.apache.spark.sql.execution.columnar.expressions.{
  ExpressionConverter,
  NativeJsonExpression
}
import org.apache.spark.sql.internal.StarryConf
import org.apache.spark.sql.types._

object RewriteExpressionWithGlobalDict extends Logging {

  import RewriteContext._

  def rewriteExpr(expression: Expression, useExecution: Boolean = false): Expression = {
    expression match {
      // ----- AGG EXPRS ----- //
      case aggWithAlias @ Alias(_: AggregateExpression, _) =>
        rewriteAggregateExpression(aggWithAlias)
      // ----- WINDOW EXPRS ----- //
      case alias @ Alias(_: WindowExpression, _) =>
        rewriteWindowExpression(alias)
      // ----- NO AGG EXPRS ----- //
      // direct references
      case _: AttributeReference | Alias(_: AttributeReference, _) =>
        expression.transformToEncodedRef()
      // skip encoded col
      case a @ Alias(_: LowCardDictEncoding, _) =>
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
      case aggregateExpression: AggregateExpression =>
        rewriteAggregateExpression(aggregateExpression)
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
        if (expr.children.head.hasExecDictInChildren()) {
          expr
        } else {
          expr.withNewChildren(Seq(expr.children.head.encodedRefInChildren().get))
        }
      // handle bool typed
      case expr if useExecution && !expr.containsPattern(TreePattern.AGGREGATE_EXPRESSION) =>
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
    var exprAfterRewriter = expression match {
      case alias @ Alias(agg: AggregateExpression, _) =>
        val newAgg = agg.mapChildren {
          case aggregateFunction: AggregateFunction =>
            aggregateFunction.mapChildren { child =>
              val newChild = tryDecodeDown(child, true)
              newChild.dict match {
                case Some(_: ExecutionColumnDict) =>
                  newChild.decode()
                case _ =>
                  newChild
              }
            }
          case other => tryDecodeDown(other)
        }
        if (newAgg fastEquals agg) {
          alias
        } else {
          Alias(newAgg, alias.name)(exprId = alias.exprId)
        }

      case agg: AggregateExpression =>
        val newAgg = agg.mapChildren {
          case aggregateFunction: AggregateFunction =>
            aggregateFunction.mapChildren { child =>
              val newChild = tryDecodeDown(child, true)
              newChild.dict match {
                case Some(_: ExecutionColumnDict) =>
                  newChild.decode()
                case _ =>
                  newChild
              }
            }
          case other => tryDecodeDown(other)
        }
        if (newAgg fastEquals agg) {
          agg
        } else {
          newAgg
        }
    }
    if (DictExpressionRewriteRegistry.findAggExprRewrite(exprAfterRewriter).isDefined) {
      // custom rewrite
      return DictExpressionRewriteRegistry
        .findAggExprRewrite(exprAfterRewriter)
        .get
        .rewrite(exprAfterRewriter)
    }
    exprAfterRewriter
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
      StarryConf.dictExecutionEnabled && expr.references.size == 1 && validRefDict &&
      !refDict.get.isInstanceOf[StartEndDict]
    }
    expression match {
      case namedExpr @ Alias(expr, name) if canDoDictExecution(expr) =>
        try {
          val maybeExpression = lookupExecutionExpression(expr)
          if (maybeExpression.isDefined) {
            return maybeExpression.get
          }

          val encodedRef = expr.references.head.encodedRefInChildren().get
          val nullExpr = expr.transform {
            case ar: AttributeReference =>
              BoundReference(0, ar.dataType, nullable = true)
          }
          val value = nullExpr.eval(InternalRow.fromSeq(Seq(null)))
          val boundExpr = expr.transform {
            case ar: AttributeReference =>
              AttributeReference("dict", ar.dataType, nullable = true)()
          }
          val expression = ExpressionConverter.convertToNative(boundExpr)
          expression match {
            case native: NativeJsonExpression =>
              val execDict =
                ExecutionColumnDict(encodedRef.dict.get, boundExpr, expr.dataType, native.native)
              if (value == null) {
                val transformedExpr = Alias(encodedRef, s"${name}_enc_transformed")()
                namedExpr.recordMapping(transformedExpr, execDict)
                recordExecutionExpression(expr, transformedExpr)
                transformedExpr
              } else {
                val transformedExpr = namedExpr.withNewChildren(
                  Seq(
                    CaseWhen(
                      Seq((IsNull(encodedRef), Literal.create(value, expr.dataType))),
                      LowCardDictDecode(encodedRef, execDict))))
                recordExecutionExpression(expr, transformedExpr)
                transformedExpr
              }
            case _ =>
              namedExpr
          }
        } catch {
          case e =>
            log.info(s"Compile dict sql error, skip dict execution ", e)
            namedExpr
        }

      case expr: Expression if canDoDictExecution(expr) =>
        val maybeExpression = lookupExecutionExpression(expr)
        if (maybeExpression.isDefined) {
          return maybeExpression.get
        }
        val encodedRef = expr.references.head.encodedRefInChildren().get
        val boundExpr = expr.transform {
          case ar: AttributeReference =>
            AttributeReference("dict", ar.dataType, nullable = true)()
        }
        val nullExpr = expr.transform {
          case ar: AttributeReference =>
            BoundReference(0, ar.dataType, nullable = true)
        }
        val value = nullExpr.eval(InternalRow.fromSeq(Seq(null)))
        try {
          val expression = ExpressionConverter.convertToNative(boundExpr)
          expression match {
            case native: NativeJsonExpression =>
              val execDict =
                ExecutionColumnDict(encodedRef.dict.get, boundExpr, expr.dataType, native.native)
              var transformedExpr = LowCardDictDecode(encodedRef, execDict)
              if (value == null) {
                recordExecutionExpression(expr, transformedExpr)
                transformedExpr
              } else {
                val after = CaseWhen(
                  Seq((IsNull(encodedRef), Literal.create(value, expr.dataType))),
                  transformedExpr)
                recordExecutionExpression(expr, after)
                after
              }
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
