package org.apache.spark.sql.execution.columnar.extension

import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.internal.StarryConf

case class OptimizeExchange() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan =
    plan.transformUp {
      case ShuffleExchangeExec(partitioning, child: SparkPlan, _)
          if StarryConf.removeSinglePartition &&
            partitioning.numPartitions == 1 && child.outputPartitioning.numPartitions == 1 =>
        child
      case join @ ShuffledHashJoinExec(
            leftKeys,
            rightKeys,
            _,
            _,
            _,
            left: ShuffleExchangeExec,
            right: ShuffleExchangeExec,
            _) if StarryConf.removeExchangeWhenSatisfies =>
        val leftChildPartitioning = left.child.outputPartitioning
        val rightChildPartitioning = right.child.outputPartitioning
        if (leftChildPartitioning.satisfies(ClusteredDistribution(leftKeys)) &&
            rightChildPartitioning.satisfies(ClusteredDistribution(rightKeys)) &&
            leftChildPartitioning.numPartitions == rightChildPartitioning.numPartitions) {
          join.withNewChildren(Seq(left.child, right.child))
        } else {
          join
        }
      case other => other
    }
}
