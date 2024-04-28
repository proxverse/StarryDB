package org.apache.spark.sql.execution.columnar.extension.rule

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.execution.columnar.expressions.aggregate.BitmapCountDistinctAggFunction
import org.apache.spark.sql.internal.StarryConf
import org.apache.spark.sql.types.{IntegerType, LongType}

object CountDistinctToBitmap extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!StarryConf.rewriteCountDistinctAsBitmap) {
      return plan
    }

    plan.transformUp {
      case agg: Aggregate if agg.containsPattern(TreePattern.COUNT) => transformAggregate(agg)
    }
  }

  private def transformAggregate(aggregate: Aggregate): Aggregate = {
    aggregate.transformExpressionsUp {
      case aggExpr @ AggregateExpression(Count(children), _, true, _, _)
        if children.size == 1 && (
            children.head.dataType.sameType(IntegerType) ||
            children.head.dataType.sameType(LongType)) =>
        aggExpr.copy(
          aggregateFunction = BitmapCountDistinctAggFunction(child = children.head),
          isDistinct = false)
      // if there is other distinct function, break out
      case AggregateExpression(_, _, true, _, _) =>
        return aggregate
    }
  }

}
