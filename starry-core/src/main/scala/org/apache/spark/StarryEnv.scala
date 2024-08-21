package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{StarryMemoryManager, StarryMemoryManagerMaster, StarryRPCConstants, StarryRPCMangerMasterEndpoint}
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef}
import org.apache.spark.sql.shuffle.{StarryShuffleConstants, StarryShuffleManager, StarryShuffleManagerMaster, StarryShuffleMangerMasterEndpoint}
import org.apache.spark.util.RpcUtils

case class StarryEnv(memoryManager: StarryMemoryManager, shuffleManager: StarryShuffleManager) {}

object StarryEnv extends Logging {
  @volatile private var env: StarryEnv = _

  def set(e: StarryEnv): Unit = {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: StarryEnv = {
    env
  }

  def createExecutorEnv(executorId: String): Unit = {
    if (executorId != SparkContext.DRIVER_IDENTIFIER) {
      createEnv(executorId)
    }
  }
  def createDriverEnv(): Unit = {
    createEnv(SparkContext.DRIVER_IDENTIFIER)
  }

  def createEnv(executorId: String): Unit = {

    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER
    def registerOrLookupEndpoint(
        name: String,
        endpointCreator: => RpcEndpoint): RpcEndpointRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        SparkEnv.get.rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, SparkEnv.get.conf, SparkEnv.get.rpcEnv)
      }
    }
    val memoryManagerMaster = new StarryMemoryManagerMaster(
      registerOrLookupEndpoint(
        StarryRPCConstants.STARRY_RPC_MANAGER_MASTER_ENDPOINT_NAME,
        new StarryRPCMangerMasterEndpoint(SparkEnv.get.conf)))
    val memoryManager =
      new StarryMemoryManager(executorId, SparkEnv.get.rpcEnv, memoryManagerMaster)

    val shuffleManagerMaster = new StarryShuffleManagerMaster(
      registerOrLookupEndpoint(
        StarryShuffleConstants.STARRY_SHUFFLE_MANAGER_MASTER_ENDPOINT_NAME,
        new StarryShuffleMangerMasterEndpoint(SparkEnv.get.conf)))
    val shuffleManager =
      new StarryShuffleManager(executorId, SparkEnv.get.rpcEnv, shuffleManagerMaster)
    set(new StarryEnv(memoryManager, shuffleManager))
  }

}
