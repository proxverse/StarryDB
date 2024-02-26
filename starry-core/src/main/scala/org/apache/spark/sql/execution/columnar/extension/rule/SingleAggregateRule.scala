package org.apache.spark.sql.execution.columnar.extension.rule

import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

case class SingleAggregateRule() extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case agg @ HashAggregateExec(_, _, _, _, _, _, _, _, child: HashAggregateExec) =>
      val sameGroupingNoAggExprs = agg.groupingExpressions == child.groupingExpressions &&
        agg.aggregateExpressions.isEmpty && child.aggregateExpressions.isEmpty

      val partialAndFinal = agg.aggregateExpressions.headOption.map(_.mode).contains(Final) &&
        child.aggregateExpressions.headOption.map(_.mode).contains(Partial)

      if (sameGroupingNoAggExprs || partialAndFinal) {
        agg.copy(
          aggregateExpressions = agg.aggregateExpressions.map(_.copy(mode = Complete)),
          child = child.child)
      } else {
        agg
      }
    case other => other
  }

}

