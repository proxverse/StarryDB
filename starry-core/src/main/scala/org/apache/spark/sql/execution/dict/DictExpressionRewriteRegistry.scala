package org.apache.spark.sql.execution.dict


import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Collect}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}

import scala.collection.mutable

trait DictExprRewrite {

  def rewriteFunc: PartialFunction[Expression, Expression]

  def couldRewrite(expression: Expression): Boolean = {
    rewriteFunc.isDefinedAt(expression)
  }

  def rewrite(expression: Expression): Expression = {
    rewriteFunc.applyOrElse(expression, (other: Expression) => RewriteExpressionWithGlobalDict.tryDecodeDown(other))
  }
}

object RewriteCollect extends DictExprRewrite {

  import RewriteContext._

  // do not decode collect list
  override def rewriteFunc: PartialFunction[Expression, Expression] = {
    case expr @ Alias(AggregateExpression(collect: Collect[mutable.ArrayBuffer[Any]], _, _, _, _), _)
      if collect.child.isInstanceOf[Attribute] =>
      expr.transformToEncodedRef()
  }
}

object RewriteCount extends DictExprRewrite {

  import RewriteContext._

  private val EVAL_AS_ENCODED_AGGS =
    Set("count", "bitmap_count_distinct")

  private def canEval(expression: Expression): Boolean = {
    expression.isInstanceOf[Attribute] &&
      !expression.encodedRefInChildren().flatMap(_.dict).exists(_.isInstanceOf[ExecutionColumnDict])
  }

  override def rewriteFunc: PartialFunction[Expression, Expression] = {
    case expr @ Alias(AggregateExpression(func, _, _, _, _), _)
      if func.children.forall(canEval) && EVAL_AS_ENCODED_AGGS.contains(func.prettyName) =>
      expr.transformToEncodedRef(false)
  }
}

object DictExpressionRewriteRegistry {

  private val defaultWindowRewrites: Seq[DictExprRewrite] = mutable.ArrayBuffer()

  private val defaultAggRewrites: Seq[DictExprRewrite] = RewriteCollect :: RewriteCount :: Nil

  private val defaultNonAggExprRewrites: Seq[DictExprRewrite] = mutable.ArrayBuffer()

  private val windowRewrites: mutable.Buffer[DictExprRewrite] = mutable.ArrayBuffer()

  private val aggRewrites: mutable.Buffer[DictExprRewrite] = mutable.ArrayBuffer()

  private val nonAggExprRewrites: mutable.Buffer[DictExprRewrite] = mutable.ArrayBuffer()

  def findWindowExprRewrite(expression: Expression): Option[DictExprRewrite] = {
    defaultWindowRewrites.find(_.couldRewrite(expression))
      .orElse(windowRewrites.find(_.couldRewrite(expression)))
  }

  def registerCustomWindowExprRewrite(rewrite: DictExprRewrite): Unit = {
    windowRewrites += rewrite
  }

  def findAggExprRewrite(expression: Expression): Option[DictExprRewrite] = {
    defaultAggRewrites.find(_.couldRewrite(expression))
      .orElse(aggRewrites.find(_.couldRewrite(expression)))
  }

  def registerCustomAggExprRewrite(rewrite: DictExprRewrite): Unit = {
    aggRewrites += rewrite
  }

  def findNonAggExprRewrite(expression: Expression): Option[DictExprRewrite] = {
    defaultNonAggExprRewrites.find(_.couldRewrite(expression))
      .orElse(nonAggExprRewrites.find(_.couldRewrite(expression)))
  }

  def registerCustomNonAggExprRewrite(rewrite: DictExprRewrite): Unit = {
     nonAggExprRewrites += rewrite
  }

}
