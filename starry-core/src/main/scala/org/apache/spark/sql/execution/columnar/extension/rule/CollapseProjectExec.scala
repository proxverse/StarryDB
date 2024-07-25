package org.apache.spark.sql.execution.columnar.extension.rule

import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, Attribute, CreateArray, CreateMap, CreateNamedStruct, Expression, ExtractValue, NamedExpression, OuterReference, PythonUDF, UpdateFields}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.columnar.extension.plan.{ColumnarProjectExec, ColumnarSupport}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

// Code is copied from
// org.apache.spark.sql.execution.columnar.extension.rule.CollapseProjectExec
object CollapseProjectExec extends Rule[SparkPlan] with AliasHelper {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case p1 @ ProjectExec(_, p2: ProjectExec)
      if sameProjType(p1, p2) && canCollapseExpressions(p1.projectList, p2.projectList, true) =>
      new ColumnarProjectExec(buildCleanedProjectList(p1.projectList, p2.projectList), p2.child)
  }

  def buildCleanedProjectList(
                               upper: Seq[NamedExpression],
                               lower: Seq[NamedExpression]): Seq[NamedExpression] = {
    val aliases = getAliasMap(lower)
    upper.map(replaceAliasButKeepName(_, aliases))
  }

  // either all of ColumnarProject or Not
  def sameProjType(p1: ProjectExec, p2: ProjectExec): Boolean = {
    (p1, p2) match {
      case (_: ColumnarSupport, _: ColumnarSupport) => true
      case (_: ColumnarSupport, _) => false
      case (_, _: ColumnarSupport) => false
      case _ => true
    }
  }

  /**
   * Check if we can collapse expressions safely.
   */
  def canCollapseExpressions(
                              consumers: Seq[Expression],
                              producers: Seq[NamedExpression],
                              alwaysInline: Boolean): Boolean = {
    canCollapseExpressions(consumers, getAliasMap(producers), alwaysInline)
  }

  /**
   * Check if we can collapse expressions safely.
   */
  def canCollapseExpressions(
                              consumers: Seq[Expression],
                              producerMap: Map[Attribute, Expression],
                              alwaysInline: Boolean = false): Boolean = {
    // We can only collapse expressions if all input expressions meet the following criteria:
    // - The input is deterministic.
    // - The input is only consumed once OR the underlying input expression is cheap.
    consumers.flatMap(collectReferences)
      .groupBy(identity)
      .mapValues(_.size)
      .forall {
        case (reference, count) =>
          val producer = producerMap.getOrElse(reference, reference)
          producer.deterministic && (count == 1 || alwaysInline || {
            val relatedConsumers = consumers.filter(_.references.contains(reference))
            // It's still exactly-only if there is only one reference in non-extract expressions,
            // as we won't duplicate the expensive CreateStruct-like expressions.
            val extractOnly = relatedConsumers.map(refCountInNonExtract(_, reference)).sum <= 1
            shouldInline(producer, extractOnly)
          })
      }
  }

  /**
   * Check if the given expression is cheap that we can inline it.
   */
  private def shouldInline(e: Expression, extractOnlyConsumer: Boolean): Boolean = e match {
    case _: Attribute | _: OuterReference => true
    case _ if e.foldable => true
    // PythonUDF is handled by the rule ExtractPythonUDFs
    case _: PythonUDF => true
    // Alias and ExtractValue are very cheap.
    case _: Alias | _: ExtractValue => e.children.forall(shouldInline(_, extractOnlyConsumer))
    // These collection create functions are not cheap, but we have optimizer rules that can
    // optimize them out if they are only consumed by ExtractValue, so we need to allow to inline
    // them to avoid perf regression. As an example:
    //   Project(s.a, s.b, Project(create_struct(a, b, c) as s, child))
    // We should collapse these two projects and eventually get Project(a, b, child)
    case _: CreateNamedStruct | _: CreateArray | _: CreateMap | _: UpdateFields =>
      extractOnlyConsumer
    case _ => false
  }

  private def collectReferences(e: Expression): Seq[Attribute] = e.collect {
    case a: Attribute => a
  }

  private def refCountInNonExtract(expr: Expression, ref: Attribute): Int = {
    def refCount(e: Expression): Int = e match {
      case a: Attribute if a.semanticEquals(ref) => 1
      // The first child of `ExtractValue` is the complex type to be extracted.
      case e: ExtractValue if e.children.head.semanticEquals(ref) => 0
      case _ => e.children.map(refCount).sum
    }

    refCount(expr)
  }
}
