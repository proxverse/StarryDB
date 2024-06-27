package org.apache.spark.sql.execution.columnar.extension

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.StarryConf

case class OptimizeExchange() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan =
    plan.transformUp {
      case ShuffleExchangeExec(partitioning, child: SparkPlan, _)
          if StarryConf.removeSinglePartition &&
            partitioning.numPartitions == 1 && child.outputPartitioning.numPartitions == 1 =>
        child
      case other => other
    }
}

case class OptimizeSort() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan =
    plan.transformUp {
      case window @ WindowExec(_, _, _, sort: SortExec) =>
        if (SortOrder.orderingSatisfies(
              sort.child.outputOrdering,
              window.requiredChildOrdering.head)) {
          window.withNewChildren(Seq(sort.child))
        } else {
          window
        }
      case other => other
    }
}
