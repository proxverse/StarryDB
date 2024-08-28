package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.rpc.netty.NettyUtil
import org.apache.spark.sql.internal.StarryConf
import org.apache.spark.sql.shuffle.{
  StarryShuffleConstants,
  StarryShuffleManager,
  StarryShuffleManagerMaster,
  StarryShuffleMangerMasterEndpoint
}
import org.apache.spark.util.{RpcUtils, Utils}

case class StarryEnv(
    memoryManager: StarryMemoryManager,
    shuffleManagerMaster: StarryShuffleManagerMaster,
    shuffleManagers: Seq[StarryShuffleManager]) {}

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
  def createDriverEnv(sc: SparkContext): Unit = {
    createEnv(SparkContext.DRIVER_IDENTIFIER)
    sc.cleaner.foreach(f =>
      f.attachListener(new CleanerListener {
        override def rddCleaned(rddId: Int): Unit = {}

        override def shuffleCleaned(shuffleId: Int): Unit = {
          StarryEnv.get.shuffleManagerMaster.removeShuffle(shuffleId)
        }

        override def broadcastCleaned(broadcastId: Long): Unit = {}

        override def accumCleaned(accId: Long): Unit = {}

        override def checkpointCleaned(rddId: Long): Unit = {}
      }))
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
      StarryMemoryManager(executorId, SparkEnv.get.rpcEnv, memoryManagerMaster)
    val shuffleManagerMaster = new StarryShuffleManagerMaster(
      registerOrLookupEndpoint(
        StarryShuffleConstants.STARRY_SHUFFLE_MANAGER_MASTER_ENDPOINT_NAME,
        new StarryShuffleMangerMasterEndpoint(SparkEnv.get.conf)))

    val managers =
      if (!SparkEnv.get.conf.get(StarryConf.COLUMNAR_SHUFFLE_ENABLED) || (isDriver && !Utils
            .isLocalMaster(SparkEnv.get.conf))) {
        Seq.empty[StarryShuffleManager]
      } else {
        val address = if (!isDriver) {
          NettyUtil.startServer()
        } else {
          SparkEnv.get.rpcEnv.address
        }
        Range(0, SparkEnv.get.conf.get(StarryConf.PRE_EXECUROE_SHUFFLE_INSTANCES))
          .map(
            i =>
              StarryShuffleManager(
                executorId,
                SparkEnv.get.rpcEnv,
                shuffleManagerMaster,
                SparkEnv.get.blockManager,
                address))
      }
    set(new StarryEnv(memoryManager, shuffleManagerMaster, managers))
  }

}
