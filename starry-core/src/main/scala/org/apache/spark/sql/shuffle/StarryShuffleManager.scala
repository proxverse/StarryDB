package org.apache.spark.sql.shuffle

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.internal.StarryConf
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, ShufflePushBlockId}
import org.apache.spark.util.IdGenerator
import org.apache.spark.{SparkContext, SparkEnv}
import org.roaringbitmap.RoaringBitmap

import java.io.File
import java.nio.channels.FileChannel
import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

object ShuffleMode extends Enumeration {
  type ShuffleMode = Value
  val STREAMING, BLOCKED = Value
}

class StarryShuffleManagerMaster(var driverEndpoint: RpcEndpointRef) extends Logging {

  def registerShuffleManager(executorId: String, storageEndpoint: RpcEndpointRef, rpcAddress: RpcAddress): Unit = {
    logInfo(s"Registering ShuffleManager $executorId")
    driverEndpoint.askSync[Boolean](RegisterManagerManager(executorId, storageEndpoint, rpcAddress))
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

trait ShuffleBatch {
  def bytes: Array[Byte]
}

case class InmemoryBatch(bytes: Array[Byte]) extends ShuffleBatch
case class DiskBatch(shuffleId: Int, reduceId: Int) extends ShuffleBatch {

  var position = 0L
  def writer(fileChannel: FileChannel): Unit = {
    position = fileChannel.position()
  }
  override def bytes: Array[Byte] = { Array.empty }
}

class StarryShuffleBlockId(override val name: String) extends ShufflePushBlockId(0, 0, 0, 0)
case class StarryShuffleManager(
    val executorId: String,
    rpcEnv: RpcEnv,
    master: StarryShuffleManagerMaster,
    blockManager: BlockManager,
    rpcAddress: RpcAddress)
    extends Logging {

  private val name
    : String = StarryShuffleConstants.STARRY_SHUFFLE_MANAGER_ENDPOINT_NAME + ShuffleManager.ID_GENERATOR.next
  val storageEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpoint(name, new StarryShuffleMangerEndpoint(name, rpcEnv, this))

  val maxMemoryUsage: Long = {
    val requireBytes = SparkEnv.get.conf.get(StarryConf.SHUFFLE_MANAGER_MEMORY_BYTES) // 1GB
    if (requireBytes == 0) {
      0
    } else {
      val success = SparkEnv.get.memoryManager.acquireStorageMemory(
        new StarryShuffleBlockId(s"${executorId} : ${storageEndpoint.name}"),
        requireBytes,
        MemoryMode.ON_HEAP)
      if (success) {
        requireBytes
      } else {
        logWarning(s"acquire memory ${requireBytes} failed, set memory bytes to 0")
        0L
      }
    }
  }

  private val streamingPartitionBuffer =
    mutable.Map[Int, mutable.HashMap[Int, util.ArrayList[Array[Byte]]]]()

  private val memorySummary = mutable.Map[Int, mutable.HashMap[Int, Long]]()

  private val storageImpl = new PartitionStorageImpl(maxMemoryUsage, blockManager)
  def addBatch(shuffleId: Int, reduceId: Int, batch: Array[Byte]): Unit = {
    val map = memorySummary
      .getOrElseUpdate(shuffleId, mutable.HashMap())
    val maybeLong = map
      .getOrElseUpdate(reduceId, { 0 })
    map.put(reduceId, maybeLong + batch.length)
    storageImpl.writeBatch(shuffleId, reduceId, batch)
  }

  val isFinish = new util.HashSet[Int]()

  private val finishMark = mutable.Map[Int, RoaringBitmap]()

  def setFinish(shuffleId: Int, mapId: Int, total: Int): Unit = {
    if (!finishMark.contains(shuffleId)) {
      finishMark.put(shuffleId, new RoaringBitmap())
    }
    val bitmap = finishMark.apply(shuffleId)
    bitmap.add(mapId)
    if (bitmap.getCardinality == total) {
      isFinish.add(shuffleId)
    }
  }

  def fetchBatchStreaming(shuffleId: Int, reduceId: Int): StreamingBatchMessage = {
    val i = storageImpl.queryBatch(shuffleId, reduceId)
    if (i == 0) {
      StreamingBatchMessage(Seq.empty, isFinish.contains(shuffleId))
    } else {
      val result = Range(0, i)
        .map(index => storageImpl.readBatch(shuffleId, reduceId, index))
        .map(_.get.bytes)
      storageImpl.close(shuffleId, reduceId)
      StreamingBatchMessage(result, isFinish.contains(shuffleId))
    }
  }

  def queryBatch(shuffleId: Int, reduceId: Int): Int = {
    storageImpl.queryBatch(shuffleId, reduceId)
  }

  def fetchBatch(shuffleId: Int, reduceId: Int, index: Int): BatchMessage = {
    val maybeBatch = storageImpl.readBatch(shuffleId, reduceId, index)
    if (maybeBatch.isEmpty) {
      BatchMessage(null)
    } else {
      BatchMessage(maybeBatch.get.bytes)
    }
  }

  def removeShuffle(shuffleId: Int): Unit = {
    isFinish.remove(shuffleId)
    finishMark.remove(shuffleId)
    storageImpl.close(shuffleId)
    memorySummary.remove(shuffleId)
  }

  def removePartition(shuffleId: Int, reduceId: Int): Unit = {
    // Remove from streamingPartitionBuffer if present
    streamingPartitionBuffer.get(shuffleId).foreach { reduceMap =>
      reduceMap.remove(reduceId)
      if (reduceMap.isEmpty) {
        streamingPartitionBuffer.remove(shuffleId)
      }
    }

    // Remove from memorySummary if present and clean up if empty
    memorySummary.get(shuffleId).foreach { reduceMap =>
      reduceMap.remove(reduceId)
      if (reduceMap.isEmpty) {
        memorySummary.remove(shuffleId)
      }
    }

    // Close the storage
    storageImpl.close(shuffleId, reduceId)
  }

  private def register(): Unit = {
    master.registerShuffleManager(SparkEnv.get.executorId, storageEndpoint, rpcAddress)
  }
  register()
  def ShuffleStatics(): String = {
    ""
  }

  def memoryStatics(): MemoryStaticsReply = {
    val map =
      memorySummary.map(tp => (tp._1.toLong, tp._2.values.sum)).toMap
    MemoryStaticsReply(s"$executorId:${storageEndpoint.name}", map)
  }
}

object ShuffleManager {
  val ID_GENERATOR = new IdGenerator
}

trait PartitionStorage {
  def writeBatch(shuffleId: Int, reduceId: Int, batch: Array[Byte]): Unit

  def readBatch(shuffleId: Int, reduceId: Int, batchIndex: Int): Option[ShuffleBatch]

  def queryBatch(shuffleId: Int, reduceId: Int): Int

  def close(shuffleId: Int, reduceId: Int)

  def close(shuffleId: Int)

  def readAllBatch(shuffleId: Int, reduceId: Int, batchIndex: Int): Option[ShuffleBatch]
}

import java.io._
import java.nio.ByteBuffer

case class BatchInfo(position: Long, length: Int)

class PartitionStorageImpl(val maxMemoryUsage: Long, val blockManager: BlockManager)
    extends PartitionStorage {

  private val memoryStore =
    mutable.Map[Int, mutable.HashMap[Int, mutable.ArrayBuffer[ShuffleBatch]]]()
  private val diskStore =
    mutable.Map[Int, mutable.HashMap[Int, (File, FileChannel, mutable.ListBuffer[BatchInfo])]]()
  private val totalCount = new AtomicLong(0)

  private def getOrCreateFile(
      shuffleId: Int,
      reduceId: Int): (File, FileChannel, mutable.ListBuffer[BatchInfo]) = {
    val (newFile, fileChannel, batchInfos) = diskStore
      .getOrElseUpdate(shuffleId, mutable.HashMap())
      .getOrElseUpdate(
        reduceId, {
          val newFile =
            blockManager.diskBlockManager.getFile(ShuffleBlockId(shuffleId, 0, reduceId))
          (
            newFile,
            new RandomAccessFile(newFile, "rw").getChannel,
            mutable.ListBuffer[BatchInfo]())
        })

    (newFile, fileChannel, batchInfos)
  }

  override def writeBatch(shuffleId: Int, reduceId: Int, batch: Array[Byte]): Unit = {
    val currentCount = totalCount.addAndGet(batch.length)
    if (currentCount <= maxMemoryUsage) {
      val batches = memoryStore
        .getOrElseUpdate(shuffleId, mutable.HashMap())
        .getOrElseUpdate(reduceId, mutable.ArrayBuffer())
      batches += InmemoryBatch(batch)
    } else {
      totalCount.addAndGet(-batch.length) // Rollback the addition
      val (_, fileChannel, batchInfos) = getOrCreateFile(shuffleId, reduceId)
      val buffer = ByteBuffer.wrap(batch)
      val position = fileChannel.size()
      fileChannel.position(position)
      fileChannel.write(buffer)
      batchInfos += BatchInfo(position, batch.length)
    }
  }

  override def readBatch(shuffleId: Int, reduceId: Int, batchIndex: Int): Option[ShuffleBatch] = {
    val memoryBatchesOption = memoryStore.get(shuffleId).flatMap(_.get(reduceId))
    memoryBatchesOption.flatMap(batches => batches.lift(batchIndex)) orElse {
      // 如果内存中有批次，需要从磁盘读取的索引应减去内存中的批次数量
      val adjustedBatchIndex = batchIndex - memoryBatchesOption.map(_.size).getOrElse(0)
      diskStore.get(shuffleId).flatMap(_.get(reduceId)).flatMap {
        case (_, fileChannel, batchInfos) =>
          batchInfos.lift(adjustedBatchIndex).flatMap {
            case BatchInfo(position, length) =>
              val buffer = ByteBuffer.allocate(length)
              fileChannel.position(position)
              fileChannel.read(buffer)
              buffer.flip()
              Some(InmemoryBatch(buffer.array()))
          }
      }
    }
  }

  override def readAllBatch(
      shuffleId: Int,
      reduceId: Int,
      batchIndex: Int): Option[ShuffleBatch] = {
    val memoryBatchesOption = memoryStore.get(shuffleId).flatMap(_.get(reduceId))
    memoryBatchesOption.flatMap(batches => batches.lift(batchIndex)) orElse {
      // 如果内存中有批次，需要从磁盘读取的索引应减去内存中的批次数量
      val adjustedBatchIndex = batchIndex - memoryBatchesOption.map(_.size).getOrElse(0)
      diskStore.get(shuffleId).flatMap(_.get(reduceId)).flatMap {
        case (_, fileChannel, batchInfos) =>
          batchInfos.lift(adjustedBatchIndex).flatMap {
            case BatchInfo(position, length) =>
              val buffer = ByteBuffer.allocate(length)
              fileChannel.position(position)
              fileChannel.read(buffer)
              buffer.flip()
              Some(InmemoryBatch(buffer.array()))
          }
      }
    }
  }

  override def close(shuffleId: Int, reduceId: Int): Unit = {
    if (memoryStore.contains(shuffleId)) {
      val maybeBatches = memoryStore.apply(shuffleId).remove(reduceId)
      if (maybeBatches.nonEmpty) {
        maybeBatches.get.map(t => totalCount.addAndGet(-t.bytes.length))
      }
    }
    if (diskStore.contains(shuffleId)) {
      val intToTuple = diskStore.apply(shuffleId)
      val maybeTuple = intToTuple.remove(reduceId)
      if (maybeTuple.nonEmpty) {
        val value = maybeTuple.get
        value._2.close()
        value._1.delete()
      }
    }
  }

  override def close(shuffleId: Int): Unit = {
    if (memoryStore.contains(shuffleId)) {
      memoryStore.apply(shuffleId).foreach { mem =>
        mem._2.map(t => totalCount.addAndGet(-t.bytes.length))
      }
    }
    if (diskStore.contains(shuffleId)) {
      diskStore
        .apply(shuffleId)
        .foreach { value =>
          value._2._2.close()
          value._2._1.delete()
        }
    }
  }

  override def queryBatch(shuffleId: Int, reduceId: Int): Int = {
    memoryStore.get(shuffleId).flatMap(_.get(reduceId)).map(_.size).getOrElse(0) +
      diskStore.get(shuffleId).flatMap(_.get(reduceId)).map(_._3.size).getOrElse(0)
  }
}
