package org.apache.spark.sql.execution.columnar.extension.rule

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, StarryConf, SparkPlan}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.{NativeParquetFileFormat, ParquetFileFormat}
case class ConvertParquetFileFormat(sparkSession: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan =
    if (StarryConf.nativeParquetReaderEnabled) {
      plan.transform {
        case scan @ FileSourceScanExec(
        relation @ HadoopFsRelation(_, _, _, _, _: ParquetFileFormat, _),
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _) =>
          scan.copy(
            relation = relation.copy(fileFormat = new NativeParquetFileFormat)(scan.session))
        case InMemoryTableScanExec(attributes, predicates, relation) =>
          val newPlan = apply(relation.cachedPlan)
          InMemoryTableScanExec(
            attributes,
            predicates,
            relation.copy(cacheBuilder = relation.cacheBuilder.copy(cachedPlan = newPlan)))
        case p =>
          p
      }
    } else {
      plan
    }
}
