package org.apache.spark.sql.shuffle

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.IdGenerator

import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

class StarryShuffleManagerMaster(var driverEndpoint: RpcEndpointRef) extends Logging {

  def registerShuffleManager(executorId: String, storageEndpoint: RpcEndpointRef): Unit = {
    logInfo(s"Registering ShuffleManager $executorId")
    driverEndpoint.askSync[Boolean](RegisterManagerManager(executorId, storageEndpoint))
    logInfo(s"Registered ShuffleManager $executorId")
  }

  def fetchAllMemory(): Map[String, Map[Long, Long]] = {
    driverEndpoint
      .askSync[Seq[MemoryStaticsReply]](MemoryStatics)
      .map(i => (i.endpoint, i.info))
      .toMap
  }

  def shuffleServices(): Array[(String, RpcAddress)] = {
    driverEndpoint
      .askSync[Array[(String, RpcAddress)]](FetchAllShuffleService)
  }

  def removeShuffle(shuffleId: Int): Unit = {
    driverEndpoint.send(CleanShuffle(shuffleId))
  }
}

case class StarryShuffleManager(
    val executorId: String,
    rpcEnv: RpcEnv,
    master: StarryShuffleManagerMaster) {

  var totalCount = new AtomicLong(0)

  private val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

  val storageEndpoint = rpcEnv.setupEndpoint(
    StarryShuffleConstants.STARRY_SHUFFLE_MANAGER_ENDPOINT_NAME + ShuffleManager.ID_GENERATOR.next,
    new StarryShuffleMangerEndpoint(executorId, rpcEnv, this))

  private val partitionCommits =
    mutable.Map[Int, mutable.HashMap[Int, util.ArrayList[Array[Byte]]]]()

  def addBatch(shuffleId: Int, reduceId: Int, batch: Array[Byte]): Unit = {
    totalCount.addAndGet(batch.length)
    if (!partitionCommits.contains(shuffleId)) {
      partitionCommits.put(shuffleId, new mutable.HashMap[Int, util.ArrayList[Array[Byte]]]())
    }
    if (!partitionCommits.apply(shuffleId).contains(reduceId)) {
      partitionCommits.apply(shuffleId).put(reduceId, new util.ArrayList[Array[Byte]]())

    }
    partitionCommits.apply(shuffleId).apply(reduceId).add(batch)
  }

  def queryBatch(shuffleId: Int, reduceId: Int): Int = {
    if (!partitionCommits.contains(shuffleId) || !partitionCommits
          .apply(shuffleId)
          .contains(reduceId)) {
      0
    } else {
      partitionCommits.apply(shuffleId).apply(reduceId).size()
    }
  }

  def fetchBatch(shuffleId: Int, reduceId: Int, index: Int): BatchMessage = {
    val result =
      if (!partitionCommits
            .contains(shuffleId) && partitionCommits.apply(shuffleId).contains(reduceId)) {
        throw new UnsupportedOperationException("Unfound shuffle data")
      } else {
        val list = partitionCommits.apply(shuffleId).apply(reduceId)
        val message = BatchMessage(list.get(index))
        message
      }
    result
  }

  def removeShuffle(shuffleId: Int): Unit = {
    partitionCommits
      .remove(shuffleId)
      .foreach(_.foreach(_._2.forEach(t => totalCount.addAndGet(-t.length))))
  }

  def removePartition(shuffleId: Int, reduceId: Int): Unit = {
    //    partitionCommits.remove(shuffleId)
    if (!partitionCommits.contains(shuffleId)) {
      return
    }
    val intToList = partitionCommits.apply(shuffleId)
    if (!intToList.contains(reduceId)) {
      return
    }
    intToList.remove(reduceId)
    if (intToList.isEmpty) {
      partitionCommits.remove(shuffleId)
    }
  }

  private def register(): Unit = {
    master.registerShuffleManager(storageEndpoint.name, storageEndpoint)
  }
  register()
  def ShuffleStatics(): String = {
//    native.ShuffleStatics()
    ""
  }

  def memoryStatics(): MemoryStaticsReply = {
    val map =
      partitionCommits.map(tp => (tp._1.toLong, tp._2.values.map(_.size().toLong).sum)).toMap
    MemoryStaticsReply(s"$executorId:${storageEndpoint.name}", map)
  }
}

object ShuffleManager {
  val ID_GENERATOR = new IdGenerator
}
