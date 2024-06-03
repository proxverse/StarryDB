package org.apache.spark.rpc

import com.google.gson.Gson
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.jni.NativeRPCManager
import org.apache.spark.util.IdGenerator

class StarryMemoryManagerMaster(var driverEndpoint: RpcEndpointRef) extends Logging {

  def registerMemoryManager(executorId: String, storageEndpoint: RpcEndpointRef): Unit = {
    logInfo(s"Registering MemoryManager $executorId")
    driverEndpoint.askSync[Boolean](RegisterManagerManager(executorId, storageEndpoint))
    logInfo(s"Registered MemoryManager $executorId")
  }

  private val gson = new Gson()
  def memoryStatics(): Map[String, Map[String, Map[String, Long]]] = {
    driverEndpoint
      .askSync[Seq[MemoryStaticsReply]](MemoryStatics)
      .map(e => (e.executorId, upickle.default.read[Map[String, Map[String, Long]]](e.info)))
      .toMap
  }

  def removeExecutor(executorId: String): Unit = {
    driverEndpoint.askSync[Boolean](RemoveExecutor(executorId))
  }

}
case class StarryMemoryManager(
    val executorId: String,
    rpcEnv: RpcEnv,
    master: StarryMemoryManagerMaster) {

  val native = new NativeRPCManager

  private val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

  private val storageEndpoint = rpcEnv.setupEndpoint(
    StarryRPCConstants.STARRY_RPC_MANAGER_ENDPOINT_NAME + MemoryManager.ID_GENERATOR.next,
    new StarryRPCMangerEndpoint(executorId, rpcEnv, this))

  private def reregister(): Unit = {
    master.registerMemoryManager(executorId, storageEndpoint)
  }
  reregister()
  def memoryStatics(): String = {
    native.memoryStatics()
  }

}

object MemoryManager {
  val ID_GENERATOR = new IdGenerator
}
