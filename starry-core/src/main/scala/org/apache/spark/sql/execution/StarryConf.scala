package org.apache.spark.sql.execution

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf

import java.util.concurrent.TimeUnit

object StarryConf {

  val STARRY_ENABLED = SQLConf
    .buildConf("spark.sql.starry.enabled")
    .doc("enabled starry")
    .booleanConf
    .createWithDefault(true)

  val NATIVE_PARQUET_READER_ENABLED = SQLConf
    .buildConf("spark.sql.starry.datasource.nativeParquetReaderEnabled")
    .doc("Use native parquet reader")
    .booleanConf
    .createWithDefault(true)

  val ASYNC_PARQUET_READER_ENABLED = SQLConf
    .buildConf("spark.sql.starry.datasource.asyncReaderEnabled")
    .doc("Use asynce native parquet reader")
    .booleanConf
    .createWithDefault(false)

  val COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED = SQLConf
    .buildConf("spark.sql.starry.columnar.forceShuffledHashJoin")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val COLUMNAR_ENABLED = SQLConf
    .buildConf("spark.sql.columnar.enabled")
    .doc("whether enable file splitting")
    .booleanConf
    .createWithDefault(true)

  val NATIVE_EXPRESSION_EXTENSION_CLASS =
    buildConf(
      "spark.sql.starry.expressions.NativeExpressionExtensionClass")
      .internal()
      .doc("extension convert expression class")
      .version("2.3.0")
      .stringConf
      .createOptional

  def isColumnarEnabled: Boolean = SQLConf.get.getConf(COLUMNAR_ENABLED)

  def expressionExtensionClass: Option[String] =
    SQLConf.get.getConf(NATIVE_EXPRESSION_EXTENSION_CLASS)
  def nativeParquetReaderEnabled: Boolean =
    SQLConf.get.getConf(NATIVE_PARQUET_READER_ENABLED)

  def asyncNativeParquetReaderEnabled: Boolean =
    SQLConf.get.getConf(ASYNC_PARQUET_READER_ENABLED)

  def forceShuffledHashJoin: Boolean =
    SQLConf.get.getConf(COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED)

  def isStarryEnabled: Boolean = SQLConf.get.getConf(STARRY_ENABLED)

}
