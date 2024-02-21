package org.apache.spark.sql.execution

import org.apache.spark.sql.internal.SQLConf

import java.util.concurrent.TimeUnit

object NebulaConf {

  val PQL_BUILD_DICT_ENCODED_COLUMN_ENABLED = SQLConf
    .buildConf("spark.sql.pql.lowCardCol.buildDictEncodedColumn.enabled")
    .doc("persist dict id encoded column during model build")
    .booleanConf
    .createWithDefault(false)

  val PQL_PARTITIONS = SQLConf
    .buildConf("spark.sql.pql.partitions")
    .doc("persist dict id encoded column during model build")
    .intConf
    .createWithDefault(0)

  val PQL_SOURCE_TARGET_COL_DICT_ENCODING_ENABLED = SQLConf
    .buildConf("spark.sql.pql.sourceTarget.encoding.enabled")
    .booleanConf
    .createWithDefault(false)

  val PQL_LOW_CARD_COL_DICT_ENCODING_ENABLED = SQLConf
    .buildConf("spark.sql.pql.lowCardColDict.encoding.enabled")
    .booleanConf
    .createWithDefault(false)

  val PQL_BUILD_DICT_EXECUTION_ENABLED = SQLConf
    .buildConf("spark.sql.pql.lowCardCol.execution.enabled")
    .doc("persist dict id encoded column during model build")
    .booleanConf
    .createWithDefault(true)

  val PQL_ENCODED_COL_DICT_ENCODING_ENABLED = SQLConf
    .buildConf("spark.sql.pql.encodedColDict.encoding.enabled")
    .booleanConf
    .createWithDefault(false)

  val PQL_ENCODED_COL_DICT_ENCODE_WITH_MODEL_BUILD = SQLConf
    .buildConf("spark.sql.pql.encodedColDict.encodeWithModelBuild")
    .booleanConf
    .createWithDefault(false)

  val PQL_LOW_CARD_COL_DICT_BUILD_TYPE = SQLConf
    .buildConf("spark.sql.pql.lowCardColDict.buildType")
    .doc(
      "which tables to build for low card index: " +
        "ACTIVITY - build activity table only;" +
        "ALL - build all tables;" +
        "NONE - build no tables;")
    .doc("when true, build low card index on all tables in the model; " +
      "otherwise only build index for activity table")
    .stringConf
    .createWithDefault("NONE")

  val PQL_LOW_CARD_COL_DICT_BUILD_THRESHOLD = SQLConf
    .buildConf("spark.sql.pql.lowCardColDictBuildThreshold")
    .intConf
    .createWithDefault(8 * 65535) // just a random value

  val PQL_PLAN_CACHE__ENABLED = SQLConf
    .buildConf("spark.sql.pql.planCacheEnabled")
    .booleanConf
    .createWithDefault(true) // just a random value

  val PQL_PRE_BUCKET_NUMBER_ROWS = SQLConf
    .buildConf("spark.sql.pql.preBucketNumberRows")
    .intConf
    .createWithDefault(5000000)

  val PQL_MERGE_TEMP_VIEWS = SQLConf
    .buildConf("spark.sql.pql.mergeTempViewsInBuilder")
    .doc("""
        |merge temp views during compile() in PQLBuilder
        |temp views referenced by columns/filters will be merged
        |as many as possible and references will be updated
        |""".stripMargin)
    .booleanConf
    .createWithDefault(true)

  val PQL_TMP_TBABLE_MOUNT_TO_ACTIVITY_TABLE = SQLConf
    .buildConf("spark.sql.pql.tmpTableMountToActivityTable")
    .doc("""
          |merge temp views during compile() in PQLBuilder
          |temp views referenced by columns/filters will be merged
          |as many as possible and references will be updated
          |""".stripMargin)
    .booleanConf
    .createWithDefault(true)

  val PQL_NATIVE_CALC_ENABLED = SQLConf
    .buildConf("spark.sql.pql.native.calc.enabled")
    .doc("""
          |merge temp views during compile() in PQLBuilder
          |temp views referenced by columns/filters will be merged
          |as many as possible and references will be updated
          |""".stripMargin)
    .booleanConf
    .createWithDefault(true)

  val PQL_BIG_EXPRESSION_CACHE_THRESHOLD = SQLConf
    .buildConf("spark.sql.pql.cache.bigExpressionCacheThreshold")
    .doc("cache big expression to reduce parser pressure")
    .intConf
    .createWithDefault(500)

  val PQL_COLUMN_CACHE_ENABLED = SQLConf
    .buildConf("spark.sql.pql.columnCacheEnabled")
    .doc("persist dict id encoded column during model build")
    .booleanConf
    .createWithDefault(true)

  val PQL_COLUMNAR_CACHE_INTERVAL = SQLConf
    .buildConf("spark.sql.pql.columnCacheInterval")
    .doc("column cache interval, to use to refresh cache")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(300)

  val PQL_COLUMNAR_CACHE_MAX_MISS_TIMES = SQLConf
    .buildConf("spark.sql.pql.columnCacheMaxMissTimes")
    .doc("column cache max miss time, to use to refresh cache")
    .intConf
    .createWithDefault(10)

  val PQL_COLUMNAR_CACHE_STRING_TYPE_ENABLED = SQLConf
    .buildConf("spark.sql.pql.StringColumnCacheEnabled")
    .doc("column cache max miss time, to use to refresh cache")
    .booleanConf
    .createWithDefault(true)

  val DBO_ENABLED = SQLConf
    .buildConf("spark.sql.pql.dbo.enabled")
    .doc("Enables DBO for estimation of plan statistics when set true.")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(true)

  val AUTO_ANALYZE_ENABLED = SQLConf
    .buildConf("spark.sql.pql.dbo.autoAnalyzeEnabled")
    .doc("Enables DBO for estimation of plan statistics when set true.")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(true)

  val PRE_PARTITION_ROW_NUM = SQLConf
    .buildConf("spark.sql.pql.dbo.prePartitionRowNum")
    .doc("Enables DBO for estimation of plan statistics when set true.")
    .version("2.2.0")
    .intConf
    .createWithDefault(1000000)

  val REMOVE_SINGLE_PARTITION = SQLConf
    .buildConf("spark.sql.pql.dbo.removeSinglePartition")
    .doc("Enables DBO for estimation of plan statistics when set true.")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(true)

  val REWRITER_SHORTENED = SQLConf
    .buildConf("spark.sql.pql.optimize.rewriterShortened")
    .doc("Enables DBO for estimation of plan statistics when set true.")
    .version("2.2.0")
    .booleanConf
    .createWithDefault(true)

  val USE_SHORTEST_PATH_FOR_FILTER = SQLConf
    .buildConf("spark.sql.pql.useShortestPathForFilter")
    .doc("Use shortest path to join indirect filter. This config will be removed when the feature is stable")
    .booleanConf
    .createWithDefault(true)

  val NEBULA_ENABLED = SQLConf
    .buildConf("spark.sql.nebula.enabled")
    .doc("enabled nebula")
    .booleanConf
    .createWithDefault(true)

  val NATIVE_PARQUET_READER_ENABLED = SQLConf
    .buildConf("spark.sql.nebula.datasource.nativeParquetReaderEnabled")
    .doc("Use native parquet reader")
    .booleanConf
    .createWithDefault(true)

  val ASYNC_PARQUET_READER_ENABLED = SQLConf
    .buildConf("spark.sql.nebula.datasource.asyncReaderEnabled")
    .doc("Use asynce native parquet reader")
    .booleanConf
    .createWithDefault(true)

  val COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED = SQLConf
    .buildConf("spark.sql.nebula.columnar.forceShuffledHashJoin")
    .internal()
    .booleanConf
    .createWithDefault(true)

  def nativeParquetReaderEnabled: Boolean =
    SQLConf.get.getConf(NATIVE_PARQUET_READER_ENABLED)

  def asyncNativeParquetReaderEnabled: Boolean =
    SQLConf.get.getConf(ASYNC_PARQUET_READER_ENABLED)

  def forceShuffledHashJoin: Boolean =
    SQLConf.get.getConf(COLUMNAR_FPRCE_SHUFFLED_HASH_JOIN_ENABLED)

  def isNebulaEnabled: Boolean = SQLConf.get.getConf(NEBULA_ENABLED)

}
