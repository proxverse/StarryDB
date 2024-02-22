package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveQueryExecSuite

class ColumnarAdaptiveQueryExecSuite
    extends AdaptiveQueryExecSuite
    with ColumnarSharedSparkSession {}
