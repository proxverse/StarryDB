package org.apache.spark.sql.execution.columnar.extension

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
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