package org.apache.spark.sql.columnar

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalog.StarryCatalog
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.connector.catalog.CatalogExtension
import org.apache.spark.sql.functions._

class CatalogSuite extends ColumnarSharedSparkSession {

  override protected def sparkConf = {
    val conf = new SparkConf()
    conf.set("spark.sql.catalog.starry", classOf[StarryCatalog].getName)
    conf
  }

  test("array_frequency") {
    spark.sessionState.catalogManager.setCurrentCatalog("starry")
    spark.sessionState.catalogManager
      .catalog("starry")
      .asInstanceOf[CatalogExtension]
      .setDelegateCatalog(spark.sessionState.catalogManager.catalog("spark_catalog"))

    val rows = spark
      .range(100)
      .agg(collect_list("id").as("array_column"))
      .select(expr("array_frequency(array_column)"))
      .collect()
    rows
  }

  test("arbitrary") {
    spark.sessionState.catalogManager.setCurrentCatalog("starry")
    spark.sessionState.catalogManager
      .catalog("starry")
      .asInstanceOf[CatalogExtension]
      .setDelegateCatalog(spark.sessionState.catalogManager.catalog("spark_catalog"))

    val rows = spark
      .range(100)
      .agg(expr("arbitrary(id)").as("array_column"))
      .collect()
    rows
  }
}
