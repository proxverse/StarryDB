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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}

/**
 * Gluten executor endpoint.
 */
class StarryShuffleMangerEndpoint(
    executorId: String,
    override val rpcEnv: RpcEnv,
    starryShuffleManager: StarryShuffleManager)
    extends ThreadSafeRpcEndpoint
    with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case batch: AddBatch =>
      starryShuffleManager.addBatch(batch.shuffleId, batch.reduceId, batch.byte)
    case removeShuffle: RemoveShuffle =>
      starryShuffleManager.removeShuffle(removeShuffle.shuffleId)
    case batch: RemoveShufflePartition =>
      starryShuffleManager.removePartition(batch.shuffleId, batch.reduceId)
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case FetchShuffleStatics =>
      context.reply(ShuffleStaticsReply(executorId, starryShuffleManager.ShuffleStatics()))
    case batch: AddBatch =>
      try {
        starryShuffleManager.addBatch(batch.shuffleId, batch.reduceId, batch.byte)
        context.reply(OK)
      } catch {
        case e: Throwable =>
          context.reply(Failed(e.getCause.getMessage))
      }

    case batch: QueryBatch =>
      try {
        context.reply(starryShuffleManager.queryBatch(batch.shuffleId, batch.reduceId))
      } catch {
        case e: Throwable =>
          context.reply(Failed(e.getCause.getMessage))
      }

    case batch: FetchBatch =>
      try {
        context.reply(
          starryShuffleManager.fetchBatch(batch.shuffleId, batch.reduceId, batch.number))
      } catch {
        case e: Throwable =>
          context.reply(Failed(e.getCause.getMessage))
      }

    case FetchMemoryStatics =>
      context.reply(starryShuffleManager.memoryStatics())
    case e =>
      logError(s"Received unexpected message. $e")
  }

}
