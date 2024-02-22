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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.{StarryConf, SparkPlan, joins}
import org.apache.spark.sql.{SparkSession, Strategy}

case class JoinSelectionOverrides(session: SparkSession)
    extends Strategy
    with JoinSelectionHelper
    with SQLConfHelper {
  private def isBroadcastStage(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, _: BroadcastQueryStageExec) => true
    case _ => false
  }

  def extractEqualJoinKeyCondition(
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      left: LogicalPlan,
      right: LogicalPlan,
      hint: JoinHint,
      forceShuffledHashJoin: Boolean): Seq[SparkPlan] = {
    if (isBroadcastStage(left) || isBroadcastStage(right)) {
      // equal condition
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      Seq(
        BroadcastHashJoinExec(
          leftKeys,
          rightKeys,
          joinType,
          buildSide,
          condition,
          planLater(left),
          planLater(right)))
    } else {
      // non equal condition
      // Generate BHJ here, avoid to do match in `JoinSelection` again.
      val isHintEmpty = hint.leftHint.isEmpty && hint.rightHint.isEmpty
      val buildSide = getBroadcastBuildSide(left, right, joinType, hint, !isHintEmpty, conf)
      if (buildSide.isDefined) {
        return Seq(
          joins.BroadcastHashJoinExec(
            leftKeys,
            rightKeys,
            joinType,
            buildSide.get,
            condition,
            planLater(left),
            planLater(right)))
      }

      if (forceShuffledHashJoin) {
        // Force use of ShuffledHashJoin in preference to SortMergeJoin. With no respect to
        // conf setting "spark.sql.join.preferSortMergeJoin".
        val (leftBuildable, rightBuildable) =
          (canBuildShuffledHashJoinLeft(joinType), canBuildShuffledHashJoinRight(joinType))

        if (!leftBuildable && !rightBuildable) {
          return Nil
        }
        val buildSide = if (!leftBuildable) {
          BuildRight
        } else if (!rightBuildable) {
          BuildLeft
        } else {
          getSmallerSide(left, right)
        }

        return Option(buildSide)
          .map { buildSide =>
            Seq(
              joins.ShuffledHashJoinExec(
                leftKeys,
                rightKeys,
                joinType,
                buildSide,
                condition,
                planLater(left),
                planLater(right)))
          }
          .getOrElse(Nil)
      }
      Nil
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      // If the build side of BHJ is already decided by AQE, we need to keep the build side.
      case ExtractEquiJoinKeysShim(joinType, leftKeys, rightKeys, condition, left, right, hint) =>
        extractEqualJoinKeyCondition(
          joinType,
          leftKeys,
          rightKeys,
          condition,
          left,
          right,
          hint,
          StarryConf.forceShuffledHashJoin)
      case _ => Nil
    }
  }
}

object ExtractEquiJoinKeysShim {
  type ReturnType =
    (
        JoinType,
        Seq[Expression],
        Seq[Expression],
        Option[Expression],
        LogicalPlan,
        LogicalPlan,
        JoinHint)
  def unapply(join: Join): Option[ReturnType] = {
    ExtractEquiJoinKeys.unapply(join).map {
      case (
          joinType,
          leftKeys,
          rightKeys,
          otherPredicates,
          predicatesOfJoinKeys,
          left,
          right,
          hint) =>
        (joinType, leftKeys, rightKeys, otherPredicates, left, right, hint)
    }
  }
}
