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

package org.apache.spark.sql.execution.columnar.extension

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext, _}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.columnar.extension.vector.ColumnarBatchInIterator
import org.apache.spark.sql.execution.columnar.jni.NativeColumnarExecution
import org.json4s.jackson.Serialization
import org.json4s.NoTypeHints

import scala.collection.JavaConverters._

class ZippedPartitionsPartition(idx: Int, @transient private val rdds: Seq[RDD[_]])
    extends Partition {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))

  def partitions: Seq[Partition] = partitionValues
}

class ColumnarExecutionRDD(
    @transient private val sc: SparkContext,
    planJson: String,
    var nodeIds: Array[String],
    var rdds: Array[RDD[ColumnarBatch]],
    outputAttributes: Seq[Attribute],
    sparkConf: SparkConf)
    extends RDD[ColumnarBatch](sc, rdds.map(x => new OneToOneDependency(x))) {

  @transient
  private implicit lazy val formats = Serialization.formats(NoTypeHints)

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partitions = split.asInstanceOf[ZippedPartitionsPartition].partitions
    val inputIterators: Seq[Iterator[ColumnarBatch]] = (rdds zip partitions).map {
      case (rdd, partition) => rdd.iterator(partition, context)
    }
    val beforeBuild = System.nanoTime()
    val execution = new NativeColumnarExecution(outputAttributes.toList.asJava)
    val columnarNativeIterator =
      inputIterators.map { iter =>
        new ColumnarBatchInIterator(iter.asJava)
      }.toArray
    val str = Serialization.write(sparkConf.getAllWithPrefix("spark.sql.columnar").toMap)
    execution.init(planJson, nodeIds, columnarNativeIterator, str)

    TaskContext.get().addTaskCompletionListener[Unit](t => execution.close())
    val iter = new Iterator[ColumnarBatch] {

      override def hasNext: Boolean = {
        val hasNext = execution.hasNext
        hasNext
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        val next1 = execution.next()
        next1
      }
    }
    new InterruptibleIterator(context, iter)
  }

  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}")
    }
    Array.tabulate[Partition](numParts) { i =>
      new ZippedPartitionsPartition(i, rdds)
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }

}
