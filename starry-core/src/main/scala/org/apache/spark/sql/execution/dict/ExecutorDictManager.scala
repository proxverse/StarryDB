/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.dict

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalNotification}
import org.apache.spark.broadcast.TorrentBroadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.columnar.jni.{NativeColumnVector, NativeExpressionConvert}
import org.apache.spark.sql.execution.columnar.{VeloxColumnarBatch, VeloxWritableColumnVector}
import org.apache.spark.sql.execution.dict.ExecutorDictManager.dictCache
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnVector

import java.lang
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

trait IDict extends Logging {
  def eval(totalBytes: AtomicLong): VeloxColumnarBatch
}

case class Dict(bytes: Array[Byte]) extends IDict {
  override def eval(totalBytes: AtomicLong): VeloxColumnarBatch = {
    val start = System.currentTimeMillis()
    val schema = StructType(Seq(StructField("dict", StringType)))
    val batch = VeloxColumnarBatch.createFromJson(bytes, schema)
    batch.setSchema(schema)
    log.info(
      s"create execution dict vector take time ${System.currentTimeMillis() - start}," +
        s" total bytes${totalBytes.addAndGet(bytes.length)}")
    batch
  }
}

case class ExecutionDict(dict: Long, numBlocks: Int, dataType: DataType, nativeExpression: String)
    extends IDict {
  override def eval(totalBytes: AtomicLong): VeloxColumnarBatch = {
    try {
      val start = System.currentTimeMillis()
      val batch = dictCache.get((dict, numBlocks))
      val schema = StructType(Seq(StructField("dict", dataType)))
      val result = NativeExpressionConvert.evalWithBatch(nativeExpression, batch, schema)
      log.info(
        s"create dict vector take time ${System.currentTimeMillis() - start}," +
          s" total bytes${totalBytes.addAndGet(-1)}")
      result
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        null
    }
  }
}

object ExecutorDictManager extends Logging {
  private val totalBytes = new AtomicLong(0)

  // todo make size from config
  val dictCache: LoadingCache[(java.lang.Long, java.lang.Integer), VeloxColumnarBatch] =
    CacheBuilder.newBuilder
      .maximumSize(200)
      .removalListener((notify: RemovalNotification[
        (java.lang.Long, java.lang.Integer),
        VeloxColumnarBatch]) => {
        log.info("remove dict vector cache ")
        notify.getValue.close()
      })
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader[(java.lang.Long, java.lang.Integer), VeloxColumnarBatch]() {
        override def load(key: (lang.Long, Integer)): VeloxColumnarBatch =
          loadDict(key._1, key._2)
      })

  val executionDictCache: LoadingCache[ExecutionDict, VeloxColumnarBatch] =
    CacheBuilder.newBuilder
      .maximumSize(50)
      .removalListener((notify: RemovalNotification[ExecutionDict, VeloxColumnarBatch]) => {
        log.info("remove execution dict vector cache ")
        notify.getValue.close()
      })
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader[ExecutionDict, VeloxColumnarBatch]() {
        def load(dict: ExecutionDict): VeloxColumnarBatch = {
          dict.eval(totalBytes)
        }
      })


  private def loadDict(bid: Long, numBlocks: Int): VeloxColumnarBatch = {
    val value = new TorrentBroadcast[IDict](null, bid, Some(numBlocks))
    try {
      value.value.eval(totalBytes)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        null
    }
  }

  def fetchDictVectorAddress(bid: Long, numBlocks: Int): NativeColumnVector = {
    fetchDictVector(bid, numBlocks) match {
      case readableVeloxColumnVector: VeloxWritableColumnVector =>
        readableVeloxColumnVector.getNative
    }
  }

  def fetchDictVector(bid: Long, numBlocks: Int): ColumnVector = {
    val value = new TorrentBroadcast[IDict](null, bid, Some(numBlocks))
    value.value match {
      case dict: Dict =>
        dictCache.get((bid, numBlocks)).column(0)
      case executionDict: ExecutionDict =>
        executionDictCache.get(executionDict).column(0)
    }
  }

}
