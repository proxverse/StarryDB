package org.apache.spark.rpc

import org.apache.spark.StarryEnv
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.scalatest.Assertions

class MemoryManagerSuite extends ColumnarSharedSparkSession {

  test("memory statics") {
    Assertions.assert(StarryEnv.get.memoryManager.master.memoryStatics().contains("driver"))

  }
  test("remove executor") {
    StarryEnv.get.memoryManager.master.removeExecutor("driver")
    Assertions.assert(StarryEnv.get.memoryManager.master.memoryStatics().isEmpty)


  }
}
