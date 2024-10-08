package org.apache.spark.sql.execution.dict

import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.execution.StarryContext
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.columnar.expressions.Unnest
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.ArrayType

import scala.collection.mutable

object RewriteWithGlobalDict extends Rule[LogicalPlan] with PredicateHelper {

  import RewriteContext._
  import RewriteExpressionWithGlobalDict._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val transformed = try {
      plan match {
        case subquery: Subquery =>
          subquery.withNewChildren(doApply(subquery.child) :: Nil)
        case _ =>
          doApply(plan)
      }
    } catch {
      case e: Exception =>
        logWarning("Failed to rewrite plan", e)
        plan
    }

    cleanUpMappings(transformed)
    transformed
  }

  private def couldApplyEncoding(plan: LogicalPlan): Boolean = {
    lazy val hasUnsupportedOperator = plan.exists {
      // TODO support Expand
      // Expand output attrs still point to original columns
      case _: Expand => true
      case _ => false
    }
    lazy val decodedBefore = plan.exists(_.expressions.exists {
      case _: LowCardDictDecode | _: LowCardDictDecodeArray => true
      case _ => false
    })

    StarryContext.get().isDefined &&
    !plan.containsPattern(TreePattern.COMMAND) &&
    !plan.isInstanceOf[LeafNode] &&
    !hasUnsupportedOperator &&
    !decodedBefore
  }

  private def doApply(plan: LogicalPlan): LogicalPlan = {
    if (couldApplyEncoding(plan)) {
      // 1. rewrite plan
      // 2. decode and trim plan
      val decoded = decodePlan(rewrite(plan), plan)
      if (decoded == plan || decoded.canonicalized.fastEquals(plan.canonicalized)) {
        return plan
      }

      // validate
      decoded.output
        .zip(plan.output)
        .find {
          case (dcol, col) => !dcol.dataType.sameType(col.dataType) && dcol.name != col.name
        }
        .foreach {
          case (dcol, col) =>
            throw new IllegalStateException(
              s"Plan is not decoded properly, encoded: " +
                s"$dcol ${dcol.dataType}, original: $col ${col.dataType}, $decoded")
        }
      decoded.foreach { plan =>
        for (ref <- plan.references) {
          if (!plan.children.exists(_.outputSet.contains(ref))) {
            throw new IllegalStateException(s"Broken plan, $ref is not found in input of $plan")
          }
        }
      }

      if (log.isDebugEnabled) {
        logDebug("transformed plan: ")
        logDebug(decoded.toString())
      }

      // remove tagging, save memory
      decoded
    } else {
      plan
    }
  }

  private def decodePlan(plan: LogicalPlan, originalPlan: LogicalPlan): LogicalPlan = {
    // get encoded attrs from plan mapping
    // as plan's mapping is always propagated while column tag is not
    val encodedAttrs = plan.getEncodedAttributes
    if (encodedAttrs.isEmpty) {
      return plan
    }

    val decodedProjectList = plan.output
      .zip(originalPlan.output)
      .map {
        case (encodedAttr, oriAttr) // decode if needed
            if encodedAttrs.find(_.exprId == encodedAttr.exprId).exists(
                // exec dict requires decoding
                _.execDictEncoded() ||
                // in case it actually queries the encoded col
                encodedAttr.dataType != oriAttr.dataType ) =>
          encodedAttrs
            .find(_.exprId == encodedAttr.exprId)
            .get
            .decode(Some(oriAttr))
            .asInstanceOf[NamedExpression]
        case (encodedAttr, oriAttr) if encodedAttr.name != oriAttr.name => // align name if needed
          Alias(encodedAttr, oriAttr.name)(oriAttr.exprId)
        case other => other._1
      }

    if (decodedProjectList == plan.output) {
      plan
    } else {
      val decodedPlan = Project(decodedProjectList, plan)
      decodedPlan
    }
  }

  private def rewrite(plan: LogicalPlan): LogicalPlan = {
    RewriteContext.transformWithContext(plan) {
      case r: LogicalRelation =>
        loadDictAndEncodeRelation(r)
      case r: InMemoryRelation =>
        loadDictAndEncodeRelation(r)
      case p: Project =>
        Project(
          p.projectList
            .map(rewriteExpr(_, useExecution = true).asInstanceOf[NamedExpression])
            .distinct,
          p.child)
      case expand: Expand =>
        expand.mapExpressions(rewriteExpr(_, useExecution = true))
      case generate: Generate =>
        generate.mapExpressions(rewriteExpr(_, useExecution = true))
      case agg: Aggregate =>
        agg.mapExpressions {
          case ar: AttributeReference if ar.hasExecDictInChildren() =>
            tryDecodeDown(ar)
          case a @ Alias(ar: AttributeReference, _) if ar.hasExecDictInChildren() =>
            tryDecodeDown(a)
          case other => rewriteExpr(other)
        }
      case join: Join =>
        rewriteJoin(join)
      case generate: Generate =>
        rewriteGenerate(generate)
      case Window(windowExpressions, partitionSpec, orderSpecs, child) =>
        val newWindowExprs = windowExpressions.map(rewriteExpr(_).asInstanceOf[NamedExpression])
        val newPartitions = partitionSpec.map(rewriteExpr(_))
        val newOrderSpecs = orderSpecs.map(rewriteExpr(_).asInstanceOf[SortOrder])
        val newWindow = Window(newWindowExprs, newPartitions, newOrderSpecs, child)
        newWindow
      case filter: Filter =>
        rewriteFilter(filter)
      case other =>
        other.mapExpressions(tryDecodeDown(_))
    }
  }

  private def rewriteJoin(join: Join): LogicalPlan = {
    var isEqualJoin = true
    val newCond = join.condition.map {
      _.transformDown {
        // allowed exprs in equal join
        case equal @ EqualTo(l: AttributeReference, r: AttributeReference) =>
          val newEqual = equal.transformToEncodedRef(false).asInstanceOf[EqualTo] // to encoded
          // falllback to decoded if not of the same encoding
          if (newEqual.left.dict != newEqual.right.dict) {
            EqualTo(l.decodeInChildren(), r.decodeInChildren())
          } else {
            newEqual
          }
        case and: And => and
        case ar: AttributeReference => ar
        // unexpected exprs found, this is an non-equi join
        case other =>
          isEqualJoin = false
          other
      }
    }
    if (isEqualJoin) {
      join.copy(condition = newCond)
    } else {
      join.mapExpressions(tryDecodeDown(_, false))
    }
  }

  private def rewriteFilter(filter: Filter): LogicalPlan = {
    if (splitConjunctivePredicates(filter.condition).size > 1) {
      val conditions = splitConjunctivePredicates(filter.condition)
      val (isNotNull, notNull) =
        conditions.partition(e =>
          e.exists(e => e.isInstanceOf[IsNotNull] || e.isInstanceOf[IsNull]))
      val (tobeDecode, other) = notNull
        .partition(_.references.size == 1)
      val expressions = tobeDecode
        .map(m => (m.references.head, m))
        .groupBy(_._1)
        .map(t => t._2.map(_._2).reduce(And))
      val newCondition =
        (expressions.map(tryDecodeDown(_, true)) ++ (isNotNull ++ other).map(tryDecodeDown(_, true)))
          .reduce(And)
      Filter(newCondition, filter.child)
    } else if (splitDisjunctivePredicates(filter.condition).size > 1) {
      val conditions = splitDisjunctivePredicates(filter.condition)
      val (isNotNull, notNull) =
        conditions.partition(e =>
          e.exists(e => e.isInstanceOf[IsNotNull] || e.isInstanceOf[IsNull]))
      val (tobeDecode, other) = notNull
        .partition(_.references.size == 1)
      val expressions = tobeDecode
        .map(m => (m.references.head, m))
        .groupBy(_._1)
        .map(t => t._2.map(_._2).reduce(Or))
      val newCondition =
        (expressions.map(tryDecodeDown(_, true)) ++ (isNotNull ++ other).map(tryDecodeDown(_, true)))
          .reduce(Or)
      Filter(newCondition, filter.child)
    } else {
      filter.condition match {
        case _ @ IsNull(_: AttributeReference) =>
          filter.copy(condition = tryDecodeDown(filter.condition))
        case _ @ IsNotNull(_: AttributeReference) =>
          filter.copy(condition = tryDecodeDown(filter.condition))
        case _ =>
          Filter(tryDecodeDown(filter.condition, true), filter.child)
      }
    }
  }

  private def rewriteGenerate(generate: Generate): LogicalPlan = {
    val hasUnsupportedExpr = (e: Expression) => {
      var has = false
      e.foreach {
        // concat: all child should be of the same type
        case concat: Concat
            if concat.children.exists(_.dataType.isInstanceOf[ArrayType]) &&
              concat.children.map(_.dataType).toSet.size == 1 =>
        case slice: Slice =>
        case unnest: Unnest =>
        case size: Size =>
        case att: AttributeReference =>
        case literal: Literal =>
        case alias: Alias =>
        case subtract: Subtract =>
        case arrays_zip: ArraysZip =>
        case arrays_zip: Inline =>
        case other => has = true
      }
      has
    }
    generate match {
      case generate @ Generate(generator: CollectionGenerator, _, _, _, _, _) =>
        if (!hasUnsupportedExpr(generator)) {
          val encodedGenerator = generator.transformToEncodedRef().asInstanceOf[Generator]
          val newOutputs = encodedGenerator.elementSchema.toAttributes
            .zip(generate.generatorOutput)
            .map { case (newAttr, oriAttr) => (newAttr.withName(oriAttr.name), oriAttr) }
            .map {
              case (newAttr, oriAttr) if newAttr.dataType == oriAttr.dataType =>
                newAttr
                  .withExprId(oriAttr.exprId) // simply copy the exprid if no encoded col involved
              case (newAttr, oriAttr) if newAttr.dataType != oriAttr.dataType => // encoded col
                oriAttr.recordMapping(newAttr, encodedGenerator.dict.get)
                newAttr
            }
          generate.copy(generator = encodedGenerator, generatorOutput = newOutputs)
        } else {
          generate.copy(generator = tryDecodeDown(generator).asInstanceOf[Generator])
        }
      case other => other
    }
  }

}
