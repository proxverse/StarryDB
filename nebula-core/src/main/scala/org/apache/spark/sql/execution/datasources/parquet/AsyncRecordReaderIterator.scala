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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch

import java.io.Closeable
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

/**
 * An adaptor from a Hadoop [[RecordReader]] to an [[Iterator]] over the values returned.
 *
 * Note that this returns [[Object]]s instead of [[InternalRow]] because we rely on erasure to pass
 * column batches by pretending they are rows.
 */

object AsyncRecordReaderIterator {
  val executor = new ThreadPoolExecutor(
    0,
    Integer.MAX_VALUE,
    600L,
    TimeUnit.SECONDS,
    new SynchronousQueue[Runnable])

  def submit(callable: Callable[Boolean]): java.util.concurrent.Future[Boolean] = {
    executor.submit(callable)
  }

}
class AsyncRecordReaderIterator[T](private[this] var rowReader: RecordReader[_, T])
    extends Iterator[T]
        with Closeable
        with Logging {
  private[this] val finished = new AtomicBoolean(false)
  private[this] var start = false
  private val queue: BlockingQueue[T] = new LinkedBlockingQueue[T](20)
  var totalRowCount: Long = _
  var consumerCount = 0
  var error: Throwable = _
  var thread: Thread = _

  val call = new Callable[Boolean] {
    override def call(): Boolean = {
      try {
        while (!finished.get()) {
          if (!rowReader.nextKeyValue) {
            finished.set(true)
          } else {
            val value = rowReader.getCurrentValue
            queue.put(value)
          }
        }
      } catch {
        case e: Throwable =>
          logError("async reader error ", e)
          error = e
          thread.interrupt()
      } finally {
        rowReader.close()
      }
      true
    }
  }

  var readFuture: java.util.concurrent.Future[Boolean] = _
  private def initialize(): Unit = {
    TaskContext.get().taskMetrics()
    val reader = rowReader.asInstanceOf[NativeVectorizedParquetRecordReader2]
    totalRowCount = reader.totalRowCount()
    thread = Thread.currentThread()
    readFuture = AsyncRecordReaderIterator.submit(call)
  }

  override def hasNext: Boolean = {
    if (!start) {
      initialize()
      start = true
    }
    consumerCount < totalRowCount
  }

  var before: VeloxColumnarBatch = _
  override def next(): T = {
    if (!start) {
      initialize()
      start = true
    }
    if (before != null) { before.close() }
    try {
      before = queue.take().asInstanceOf[VeloxColumnarBatch]
    } catch {
      case interruptedException: InterruptedException =>
        if (error != null) {
          throw error
        }
      case other => throw other
    }
    consumerCount += before.numRows()
    before.asInstanceOf[T]
  }

  override def map[B](f: T => B): Iterator[B] with Closeable =
    new Iterator[B] with Closeable {
      override def hasNext: Boolean = AsyncRecordReaderIterator.this.hasNext
      override def next(): B = f(AsyncRecordReaderIterator.this.next())
      override def close(): Unit = AsyncRecordReaderIterator.this.close()
    }

  override def close(): Unit = {
    finished.set(true)
    if (before != null) { before.close() }
    while (!queue.isEmpty) {
      val t = queue.poll(1, TimeUnit.SECONDS)
      t.asInstanceOf[VeloxColumnarBatch].close()
    }
    if (readFuture != null) {
      readFuture.get()
    }
    while (!queue.isEmpty) {
      val t = queue.take()
      t.asInstanceOf[VeloxColumnarBatch].close()
    }
  }
}
