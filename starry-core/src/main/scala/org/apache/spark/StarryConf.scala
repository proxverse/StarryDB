package org.apache.spark.sql.internal

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.OptionalConfigEntry
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf.buildConf
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

object StarryConf {

  val STARRY_ENABLED = SQLConf
    .buildConf("spark.sql.starry.enabled")
    .doc("enabled starry")
    .booleanConf
    .createWithDefault(false)

  val NATIVE_PARQUET_READER_ENABLED = SQLConf
    .buildConf("spark.sql.starry.datasource.nativeParquetReaderEnabled")
    .doc("Use native parquet reader")
    .booleanConf
    .createWithDefault(true)

  val ASYNC_PARQUET_READER_ENABLED = SQLConf
    .buildConf("spark.sql.starry.datasource.asyncReaderEnabled")
    .doc("Use asynce native parquet reader")
    .booleanConf
    .createWithDefault(true)

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
    buildConf("spark.sql.starry.expressions.NativeExpressionExtensionClass")
      .internal()
      .doc("extension convert expression class")
      .version("2.3.0")
      .stringConf
      .createOptional

  val REWRITE_COUNT_DISTINCT_AS_BITMAP = SQLConf
    .buildConf("spark.sql.starry.expressions.rewriteCountDistinctAsBitmap")
    .doc("rewrite count distinct as bitmap count distinct")
    .booleanConf
    .createWithDefault(true)

  val ROOT_MEMORY_CAPACITY =
    buildConf("spark.sql.starry.maxRootMemoryBytes")
      .internal()
      .doc("root memory capacity")
      .version("2.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val DEFAULT_QUERY_MEMORY_CAPACITY =
    buildConf("spark.sql.starry.defaultQueryMemoryBytes")
      .internal()
      .doc("default query memory capacity for task")
      .version("2.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val MAX_QUERY_MEMORY_CAPACITY =
    buildConf("spark.sql.starry.maxQueryMemoryBytes")
      .internal()
      .doc("max query memory capacity for task")
      .version("2.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val COLUMNAR_SORT_MERGE_JOIN_ENABLED = SQLConf
    .buildConf("spark.sql.starry.columnar.sortMergeJoinEnabled")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val COLUMNAR_REWRITE_SORT_MERGE_JOIN_ENABLED = SQLConf
    .buildConf("spark.sql.starry.columnar.rewriteSortMergeJoinEnabled")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val COLUMNAR_REWRITE_BROADCAST_JOIN_ENABLED = SQLConf
    .buildConf("spark.sql.starry.columnar.rewriteBroadcastJoinEnabled")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val REMOVE_SINGLE_PARTITION = SQLConf
    .buildConf("spark.sql.starry.columnar.removeSinglePartition")
    .doc("移除一个分区的 exchange")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(true)

  val NEW_DATE_DIFF_ENABLED = SQLConf
    .buildConf("spark.sql.starry.columnar.newDateDiffEnabled")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val DICT_EXECUTION_ENABLED = SQLConf
    .buildConf("spark.sql.starry.allowDictExprExecution")
    .booleanConf
    .createWithDefault(true)

  def dictExecutionEnabled: Boolean =
    SQLConf.get.getConf(DICT_EXECUTION_ENABLED)

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

  def rewriteSMJEnabled: Boolean =
    SQLConf.get.getConf(COLUMNAR_REWRITE_SORT_MERGE_JOIN_ENABLED)

  def sMJEnabled: Boolean =
    SQLConf.get.getConf(COLUMNAR_SORT_MERGE_JOIN_ENABLED)

  def removeSinglePartition: Boolean = SQLConf.get.getConf(REMOVE_SINGLE_PARTITION)

  def rewriteCountDistinctAsBitmap: Boolean =
    SQLConf.get.getConf(REWRITE_COUNT_DISTINCT_AS_BITMAP)

  def newDateDiffEnabled: Boolean =
    SQLConf.get.getConf(NEW_DATE_DIFF_ENABLED)

  def getAllConf(sparkConf: Map[String, String], prefix: String): Map[String, Any] = {
    sparkConf
      .filter(_._1.startsWith(prefix))
      .map { en =>
        if (SQLConf.containsConfigKey(en._1)) {
          val value = SQLConf.getConfigEntry(en._1) match {
            case opt: OptionalConfigEntry[Any] =>
              SQLConf
                .getConfigEntry(en._1)
                .valueConverter
                .apply(en._2)
                .asInstanceOf[Option[Any]]
                .get
            case other =>
              SQLConf.getConfigEntry(en._1).valueConverter.apply(en._2)
          }
          (en._1, value)
        } else {
          (en._1, en._2)
        }
      }
  }
  private implicit lazy val formats = Serialization.formats(NoTypeHints)

  def getAllConfJson(sparkConf: SparkConf, prefix: String): String = {
    Serialization.write(StarryConf.getAllConf(sparkConf.getAll.toMap, prefix))
  }
}
