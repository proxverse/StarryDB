package org.apache.spark.sql.execution.columnar.extension.rule

import org.apache.spark.sql.catalyst.expressions.{ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable

object ProjectRemoveDupColumn extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case project @ Project(pList, child) if pList.size > project.outputSet.size =>
      val newPList = mutable.Buffer.empty[NamedExpression]
      val visited = mutable.Set.empty[ExprId]
      pList.foreach { named =>
        if (!visited.contains(named.exprId)) {
          newPList += named
          visited += named.exprId
        }
      }
      project.copy(projectList = newPList)
  }
}
