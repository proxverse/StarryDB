package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.shuffle.{AddBatch, Finish}

object ShuffleUtils {

  def run(
      inputRDD: RDD[InternalRow],
      shuffleServices: Array[(String, RpcAddress)],
      shuffleId: Int): Unit = {
    SQLExecution.withThreadLocalCaptured[Array[Unit]](
      SparkSession.active,
      BroadcastExchangeExec.executionContext) {
      val total = inputRDD.partitions.length
      inputRDD
        .mapPartitionsWithIndexInternal(
          (index, iter) => {
            val rpcs = shuffleServices.map { address =>
              SparkEnv.get.rpcEnv.setupEndpointRef(address._2, address._1)
            }
            val partitions = rpcs.length
            iter.foreach { row =>
              val tp = (row.getInt(0), row)
              val partition = tp._1
              val start = System.nanoTime()
              val bytes = tp._2.getBinary(1)
              rpcs
                .apply(partition % partitions)
                .send(AddBatch(shuffleId, partition, bytes))
            }
            rpcs.foreach(rpc => rpc.send(Finish(shuffleId, index, total)))
            Iterator.empty
          },
          isOrderSensitive = false)
        .collect()
    }
  }
}
