package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf

object ColumnarConf {
  val COLUMNAR_ENABLED = SQLConf
    .buildConf("spark.sql.columnar.enabled")
    .doc("whether enable file splitting")
    .booleanConf
    .createWithDefault(true)

  val SERVICE_ADDR = SQLConf
    .buildConf("spark.jqas.service.address")
    .doc("address to jqas service")
    .stringConf
    .createWithDefault("localhost")

  val JQAS_FAIL_BACK = SQLConf
    .buildConf("spark.jqas.fallback")
    .doc("skip jqas in executors")
    .booleanConf
    .createWithDefault(false)

  val ORC_FILTER_PUSHDOWN = SQLConf
    .buildConf("spark.jqas.orc.filterPushdown")
    .doc("skip jqas in executors")
    .booleanConf
    .createWithDefault(true)

  val SUPPORT_COMPLEX_TYPE = SQLConf
    .buildConf("spark.jqas.orc.supportComplexType")
    .doc("skip jqas in executors")
    .booleanConf
    .createWithDefault(true)

  val LOGGER_ENABLED = SQLConf
    .buildConf("spark.jqas.orc.loggerEnabled")
    .doc("skip jqas in executors")
    .booleanConf
    .createWithDefault(true)
  val AGGREGATE_PUSHDOWN_ENABLED = SQLConf
    .buildConf("spark.jqas.aggPushDown.enabled")
    .doc("skip jqas in executors")
    .booleanConf
    .createWithDefault(false)

  val NATIVE_EXPRESSION_EXTENSION_CLASS =
    buildConf(
      "spark.sql.columnar.extended.expressions.transformer.NativeExpressionExtensionClass")
      .internal()
      .doc("extension convert expression class")
      .version("2.3.0")
      .stringConf
      .createOptional

  def isColumnarEnabled: Boolean = SQLConf.get.getConf(COLUMNAR_ENABLED)

  def expressionExtensionClass: Option[String] =
    SQLConf.get.getConf(NATIVE_EXPRESSION_EXTENSION_CLASS)

}
