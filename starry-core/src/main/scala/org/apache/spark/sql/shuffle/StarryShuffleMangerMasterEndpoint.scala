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

package org.apache.spark.sql.shuffle

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.netty.NettyUtil
import org.apache.spark.rpc.{
  RpcAddress,
  RpcCallContext,
  RpcEndpointRef,
  RpcEnv,
  ThreadSafeRpcEndpoint
}
import org.apache.spark.util.{RpcUtils, ThreadUtils}
import org.apache.spark.{SparkConf, SparkEnv}

import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag

class StarryShuffleMangerMasterEndpoint(conf: SparkConf)
    extends ThreadSafeRpcEndpoint
    with Logging {
  private val executorDataMap =
    new ConcurrentHashMap[String, util.ArrayList[(RpcEndpointRef, RpcAddress)]]
  var allShuffleService = Array.empty[(String, RpcAddress)]
  var allEndpoints = Seq.empty[RpcEndpointRef]

  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  val timeout = RpcUtils.askRpcTimeout(conf)

  import scala.concurrent.Future
  def askAll[T: ClassTag](message: Any): Seq[T] = {
    val futures =
      allEndpoints.map(c => c.ask[T](message))
    implicit val sameThread = ThreadUtils.sameThread
    val cbf =
      implicitly[CanBuildFrom[Seq[Future[T]], T, Seq[T]]]
    timeout.awaitResult(Future.sequence(futures)(cbf, ThreadUtils.sameThread))
  }

  def sendAll[T: ClassTag](message: Any): Unit = {
    allEndpoints.foreach(c => c.send(message))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CleanShuffle(shuffleId) =>
      sendAll(RemoveShuffle(shuffleId))
    case e =>
      logError(s"Received unexpected message. $e")
  }
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterManagerManager(executorId, rpc, address) =>
      if (!executorDataMap.contains(executorId)) {
        executorDataMap.put(executorId, new util.ArrayList[(RpcEndpointRef, RpcAddress)]())
      }
      executorDataMap
        .get(executorId)
        .add((rpc, address))
      updateEndpoint()
      context.reply(true)
    case FetchAllShuffleService =>
      context.reply(allShuffleService)

    case MemoryStatics =>
      context.reply(askAll[MemoryStaticsReply](FetchMemoryStatics))
    case RemoveExecutor(executorId) =>
      if (executorDataMap.containsKey(executorId)) {
        executorDataMap.remove(executorId)
      }
      updateEndpoint()
      context.reply(true)
    case e =>
      logError(s"Received unexpected message. $e")
  }

  private def updateEndpoint(): Unit = {
    allEndpoints = executorDataMap
      .values()
      .asScala
      .flatMap(_.asScala)
      .map(_._1)
      .toSeq
    allShuffleService = executorDataMap
      .values()
      .asScala
      .flatMap(list =>
        list.asScala
          .map(e => (e._1.name, e._2)))
      .toArray
  }

  override def onStart(): Unit = {
    logInfo(s"Initialized GlutenDriverEndpoint.")
  }
}
