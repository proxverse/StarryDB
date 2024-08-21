package org.apache.spark.sql.shuffle

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.IdGenerator

import java.util
import scala.collection.mutable

class StarryShuffleManagerMaster(var driverEndpoint: RpcEndpointRef) extends Logging {

  def registerShuffleManager(executorId: String, storageEndpoint: RpcEndpointRef): Unit = {
    logInfo(s"Registering ShuffleManager $executorId")
    driverEndpoint.askSync[Boolean](RegisterManagerManager(executorId, storageEndpoint))
    logInfo(s"Registered ShuffleManager $executorId")
  }

  def removeExecutor(executorId: String): Unit = {
    driverEndpoint.askSync[Boolean](RemoveExecutor(executorId))
  }

}

case class StarryShuffleManager(
    val executorId: String,
    rpcEnv: RpcEnv,
    master: StarryShuffleManagerMaster) {

  private val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

  val storageEndpoint = rpcEnv.setupEndpoint(
    StarryShuffleConstants.STARRY_SHUFFLE_MANAGER_ENDPOINT_NAME + ShuffleManager.ID_GENERATOR.next,
    new StarryShuffleMangerEndpoint(executorId, rpcEnv, this))

  private val partitionCommits =
    mutable.Map[(Int, Int), util.ArrayList[UTF8String]]()

  def addBatch(shuffleId: Int, reduceId: Int, batch: UTF8String): Unit = {
    if (!partitionCommits.contains((shuffleId, reduceId))) {
      partitionCommits.put((shuffleId, reduceId), new util.ArrayList[UTF8String]())
    }
    partitionCommits.apply((shuffleId, reduceId)).add(batch)
  }

  def queryBatch(shuffleId: Int, reduceId: Int): Int = {
    if (!partitionCommits.contains((shuffleId, reduceId))) {
      0
    } else {
      partitionCommits.apply((shuffleId, reduceId)).size()
    }
  }

  def fetchBatch(shuffleId: Int, reduceId: Int, index: Int): BatchMessage = {
    val tuple = (shuffleId, reduceId)
    val result = if (!partitionCommits.contains(tuple)) {
      throw new UnsupportedOperationException("Unfound shuffle data")
    } else {
      val list = partitionCommits.apply(tuple)
      val message = BatchMessage(list.get(index))
      if (index == list.size() - 1) {
        partitionCommits.remove(tuple)
      }
      message
    }

    result
  }

  def removeShuffle(shuffleId: Int): Unit = {
//    partitionCommits.remove(shuffleId)
  }

  private def reregister(): Unit = {
    master.registerShuffleManager(executorId, storageEndpoint)
  }
  reregister()
  def ShuffleStatics(): String = {
//    native.ShuffleStatics()
    ""
  }

}

object ShuffleManager {
  val ID_GENERATOR = new IdGenerator
}
