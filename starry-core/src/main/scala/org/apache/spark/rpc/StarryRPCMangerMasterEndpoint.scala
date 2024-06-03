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

package org.apache.spark.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}
import org.apache.spark.{SparkConf, SparkEnv}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag


class StarryRPCMangerMasterEndpoint(conf: SparkConf) extends IsolatedRpcEndpoint with Logging {
  private val executorDataMap = new ConcurrentHashMap[String, RpcEndpointRef]
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  val timeout = RpcUtils.askRpcTimeout(conf)

  import scala.concurrent.Future
  def askAll[T: ClassTag](message: Any): Seq[T] = {
    val futures =
      executorDataMap.values.asScala.toSeq.map(c => c.ask[T](message))
    implicit val sameThread = ThreadUtils.sameThread
    val cbf =
      implicitly[CanBuildFrom[Seq[Future[T]], T, Seq[T]]]
    timeout.awaitResult(Future.sequence(futures)(cbf, ThreadUtils.sameThread))
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterManagerManager(executorId, sender) =>
      if (executorDataMap.contains(executorId)) {
        context.sendFailure(new IllegalStateException(s"Duplicate executor ID: $executorId"))
      } else {
        executorDataMap.put(executorId, sender)
      }
      context.reply(true)
    case MemoryStatics =>
      val strings = askAll[MemoryStaticsReply](FetchMemoryStatics)
      context.reply(strings)
    case RemoveExecutor(executorId) =>
      if (executorDataMap.containsKey(executorId))  {
        executorDataMap.remove(executorId)
      }
      context.reply(true)
    case e =>
      logError(s"Received unexpected message. $e")
  }
  override def onStart(): Unit = {
    logInfo(s"Initialized GlutenDriverEndpoint.")
  }
}
