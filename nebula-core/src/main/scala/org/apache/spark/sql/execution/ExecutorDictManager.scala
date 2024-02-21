///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.spark.sql.execution
//
//import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalNotification}
//import org.apache.spark.broadcast.TorrentBroadcast
//import org.apache.spark.internal.Logging
//import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
//import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
//import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
//import org.apache.spark.sql.vectorized.ColumnVector
//
//import java.lang
//import java.util.concurrent.TimeUnit
//import java.util.concurrent.atomic.AtomicLong
//
//trait IDict
//
//case class Dict(bytes: Array[Byte]) extends IDict
//
//case class ExecutionDict(dict: Long, numBlocks: Int, dataType: DataType,
//                         nativeExpression: String) extends IDict
//
//object ExecutorDictManager extends Logging {
//  private val totalBytes = new AtomicLong(0)
//
//  // todo make size from config
//  val dictCache: LoadingCache[(java.lang.Long, java.lang.Integer), VeloxColumnarBatch] = CacheBuilder.newBuilder
//    .maximumSize(200)
//    .removalListener(
//      (notify: RemovalNotification[(java.lang.Long, java.lang.Integer), VeloxColumnarBatch]) => {
//        log.info("remove dict vector cache ")
//        notify.getValue.close()
//      })
//    .expireAfterAccess(1, TimeUnit.HOURS)
//    .build(new CacheLoader[(java.lang.Long, java.lang.Integer), VeloxColumnarBatch]() {
//      override def load(key: (lang.Long, Integer)): VeloxColumnarBatch = loadDict(key._1, key._2)
//    })
//
//  val executionDictCache: LoadingCache[ExecutionDict, VeloxColumnarBatch] = CacheBuilder.newBuilder
//    .maximumSize(50)
//    .removalListener(
//      (notify: RemovalNotification[ExecutionDict, VeloxColumnarBatch]) => {
//        log.info("remove execution dict vector cache ")
//        notify.getValue.close()
//      })
//    .expireAfterAccess(1, TimeUnit.HOURS)
//    .build(new CacheLoader[ExecutionDict, VeloxColumnarBatch]() {
//      def load(dict: ExecutionDict): VeloxColumnarBatch = {
//        eval(dict)
//      }
//    })
//
//  private def eval(dict: ExecutionDict) = {
//    var exprHandle = 0L
//    try {
//      val start = System.currentTimeMillis()
//      val batch = dictCache.get((dict.dict, dict.numBlocks))
//      exprHandle = NativeExpressionConvert.nativeDeserializeExpr(dict.nativeExpression)
//      val resultHandle = NativeExpressionConvert.nativeEvalWithBatch(exprHandle, batch.getObjectPtr)
//      val schema = StructType(Seq(StructField("dict", dict.dataType)))
//      log.info(
//        s"create dict vector take time ${System.currentTimeMillis() - start}," +
//          s" total bytes${totalBytes.addAndGet(-1)}")
//      ColumnBatchUtils
//        .createBatchFromAddress(resultHandle, schema)
//    } catch {
//      case e: Throwable =>
//        e.printStackTrace()
//        null
//    } finally {
//      NativeExpressionConvert.nativeReleaseHandle(exprHandle)
//    }
//  }
//
//  private def loadDict(bid: Long, numBlocks: Int): VeloxColumnarBatch = {
//    val value = new TorrentBroadcast[IDict](null, bid, Some(numBlocks))
//    try {
//      value.value match {
//        case dict: Dict =>
//          createDictBatch(dict)
//        case executionDict: ExecutionDict =>
//          eval(executionDict)
//      }
//    } catch {
//      case e: Throwable =>
//        e.printStackTrace()
//        null
//    }
//  }
//
//  private def createDictBatch(dict: Dict): VeloxColumnarBatch = {
//    val start = System.currentTimeMillis()
//    val schema = StructType(Seq(StructField("dict", StringType)))
//    val c_schema =
//      ArrowCSchemaUtils.exportSchemaToAddress(SparkArrowUtil.toArrowSchema(schema, null))
//    val batch =
//      GlutenColumnBatchUtils
//        .createBatchFromAddress(GlutenColumnBatchUtils.deserialize(dict.bytes, c_schema), schema);
//    ArrowCSchemaUtils.releaseMemory(c_schema)
//    log.info(
//      s"create execution dict vector take time ${System.currentTimeMillis() - start}," +
//        s" total bytes${totalBytes.addAndGet(dict.bytes.length)}")
//    batch
//  }
//
//  def fetchDictVectorAddress(bid: Long, numBlocks: Int): Long = {
//    fetchDictVector(bid, numBlocks) match {
//      case readableVeloxColumnVector: ReadableVeloxColumnVector =>
//        readableVeloxColumnVector.objectAddress
//      case constantVeloxColumnVector: ConstantVeloxColumnVector =>
//        constantVeloxColumnVector.objectAddress
//    }
//  }
//
//  def fetchDictVector(bid: Long, numBlocks: Int): ColumnVector = {
//    val value = new TorrentBroadcast[IDict](null, bid, Some(numBlocks))
//    value.value match {
//      case dict: Dict =>
//        dictCache.get((bid, numBlocks)).column(0)
//      case executionDict: ExecutionDict =>
//        executionDictCache.get(executionDict).column(0)
//    }
//  }
//
//}
