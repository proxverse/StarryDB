package org.apache.spark.sql.execution.dict

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, Join, LocalLimit, LogicalPlan, Subquery, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNodeTag}
import org.apache.spark.sql.execution.columnar.{CachedRDDBuilder, InMemoryRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.ArrayType

import scala.collection.mutable

object RewriteContext {


  private val currentPlan_ = new ThreadLocal[PlanWithDictMapping]()

  private def currentPlan = currentPlan_.get()

  // expr tag
  private val GLOBAL_DICT_TAG = TreeNodeTag[ColumnDict]("STARRY_GLOBAL_DICT")

  // plan tag
  private type OriginalToEncodedMap = mutable.HashMap[ExprId, Attribute]
  private val GLOBAL_DICT_MAPPING_TAG = TreeNodeTag[OriginalToEncodedMap]("STARRY_GLOBAL_DICT_MAPPING")

  def transformWithContext(root: LogicalPlan)(rule: Function[LogicalPlan, LogicalPlan]): LogicalPlan = {
    try {
      currentPlan_.remove()
      transform(root)(rule)
    } finally {
      currentPlan_.remove()
    }
  }

  def transform(plan: LogicalPlan)(rule: Function[LogicalPlan, LogicalPlan]): LogicalPlan = {
    // record plan
    val afterRuleOnChildren = plan.mapChildren(transform(_)(rule))
    currentPlan_.set(afterRuleOnChildren)

    // apply rule
    val afterRule = CurrentOrigin.withOrigin(plan.origin) {
      rule.apply(afterRuleOnChildren)
    }

    // check if plan changed
    val transformed = if ((afterRuleOnChildren eq afterRule) && currentPlan.getEncodingMapping.isEmpty) {
      afterRuleOnChildren
    } else {
      afterRule
    }

    // make sure mapping tag is copied
    currentPlan.getEncodingMapping.foreach(mapping => {
      transformed.withEncodingMapping(mapping)
    })
    // copy children mapping tag if the operator is not changing the output
    transformed match {
      case _: Join | _: Filter | _: LocalLimit | _: Subquery | _: SubqueryAlias | _: GlobalLimit =>
        transformed.withChildrenEncodingMapping()
      case colUnchanged
        if colUnchanged.children.size == 1 &&
          colUnchanged.outputSet == colUnchanged.children.head.outputSet &&
          colUnchanged.getEncodedAttributes.isEmpty =>
        transformed.withChildrenEncodingMapping()
      case _ =>
    }

    transformed
  }

  private def setDictTag(expression: Expression, columnDict: ColumnDict): Unit = {
    expression.setTagValue(GLOBAL_DICT_TAG, columnDict)
  }

  private def getDictTag(expression: Expression): Option[ColumnDict] = {
    expression.getTagValue(GLOBAL_DICT_TAG)
  }

  private def setMappingTag(plan: LogicalPlan, mapping: OriginalToEncodedMap): Unit = {
    plan.setTagValue(GLOBAL_DICT_MAPPING_TAG, mapping)
  }

  private def getMappingTag(plan: LogicalPlan): Option[OriginalToEncodedMap] = {
    plan.getTagValue(GLOBAL_DICT_MAPPING_TAG)
  }

  def cleanUpMappings(root: LogicalPlan): Unit = {
    root.foreach { plan =>
      plan.unsetTagValue(GLOBAL_DICT_MAPPING_TAG)
      plan.expressions.foreach(_.unsetTagValue(GLOBAL_DICT_TAG))
    }
  }

  // expression wrapper for easy access columnDict
  implicit class ColumnWithDict(expression: Expression) {

    private[RewriteContext] def setDict(columnDict: ColumnDict): Unit = {
      setDictTag(expression, columnDict)
    }

    def dict: Option[ColumnDict] = {
      getDictTag(expression)
    }

    def isDictEncoded: Boolean = {
      getDictTag(expression).isDefined
    }

    def decode(copyNameAndExprIdForm: Option[NamedExpression] = None): Expression = {
      // avoid bug that try decoding twice
      if (dict.isEmpty || expression.exists(_.isInstanceOf[LowCardDictEncoding])) {
        return expression
      }

      val decoded: Expression = expression.dataType match {
        case ArrayType(_, _) =>
          LowCardDictDecodeArray(expression, dict.get)
        case _ =>
          LowCardDictDecode(expression, dict.get)
      }

      expression match {
        case _: NamedExpression if copyNameAndExprIdForm.isDefined =>
          Alias(decoded, copyNameAndExprIdForm.get.name)(copyNameAndExprIdForm.get.exprId)
        case namedExpression: NamedExpression =>
          Alias(decoded, namedExpression.name)()
        case _ =>
          decoded
      }
    }

    def dictInChildren(): Option[ColumnDict] = {
      currentPlan.findEncodedExprInChildren(expression).flatMap(_.dict)
    }

    def encodedRefInChildren(): Option[Expression] = {
      currentPlan.findEncodedExprInChildren(expression)
    }

    def decodeInChildren(): Expression = {
      (expression match {
        case named: NamedExpression =>
          currentPlan.findEncodedExprInChildren(expression).map(_.decode(Some(named)))
        case _ =>
          currentPlan.findEncodedExprInChildren(expression).map(_.decode())
      }).getOrElse(expression)
    }

    def recordMapping(encodedExpr: NamedExpression, dict: ColumnDict): Unit = {
      expression match {
        case named: NamedExpression => updateMapping(named, encodedExpr, dict)
        case _ => encodedExpr.setDict(dict)
      }
    }

    def transformToEncodedRef(needUpdateMapping: Boolean = true): Expression = {
      var dictUsing: Option[ColumnDict] = None
      val encoded = expression.transformUp {
        case ar: AttributeReference if !ar.isDictEncoded =>
          val encodedExpr = currentPlan.findEncodedExprInChildren(ar)
          encodedExpr.foreach(encoded => dictUsing = encoded.dict)
          encodedExpr.getOrElse(ar)
      }
      if (dictUsing.isEmpty) {
        return expression
      }

      if (needUpdateMapping) {
        encoded match {
          case namedEncoded: NamedExpression =>
            recordMapping(namedEncoded, dictUsing.get)
          case _ =>
        }
      }
      encoded.setDict(dictUsing.get)
      encoded
    }

  }

  // plan wrapper for easy access encoding mapping
  implicit class PlanWithDictMapping(val plan: LogicalPlan) {

    private[RewriteContext] def withEncodingMapping(map: mutable.HashMap[ExprId, Attribute]): LogicalPlan = {
      setMappingTag(plan, map)
      plan
    }

    def getEncodingMapping: Option[OriginalToEncodedMap] = {
      getMappingTag(plan)
    }

    def getEncodedAttributes: Seq[Attribute] = {
      getEncodingMapping.map(_.values.toSeq).getOrElse(Seq.empty)
    }

    def findEncodedExprInChildren(originalExpr: Expression): Option[Expression] = {
      for (child <- plan.children) {
        val found = getMappingTag(child).flatMap { map =>
          originalExpr match {
            case named: NamedExpression => map.get(named.exprId)
            case _ => None
          }
        }
        if (found.isDefined) {
          return found
        }
      }
      None
    }

    def withChildrenEncodingMapping(): LogicalPlan = {
      val mapping = getEncodingMapping.getOrElse(new OriginalToEncodedMap)
      plan.children
        .flatMap { child => child.getEncodingMapping }
        .foreach { map =>
          mapping ++= map
        }
      if (mapping.nonEmpty) {
        plan.withEncodingMapping(mapping)
      }
      plan
    }

  }

  private def loadDictIndexes(relation: LogicalPlan): Seq[ColumnDictIndex] = {
    val dbAndTable = relation match {
      case LogicalRelation(_, _, Some(catalogTable), _) if catalogTable.identifier.database.isDefined =>
        Some(catalogTable.identifier.database.get, catalogTable.identifier.table)
      case InMemoryRelation(_, CachedRDDBuilder(_, _, _, Some(tableName)), _) =>
        val ua = UnresolvedAttribute(tableName)
        if (ua.nameParts.size == 2) {
          Some(ua.nameParts.head, ua.nameParts.last)
        } else {
          None
        }
      case _ =>
        None
    }

    dbAndTable.map { case(db, table) =>
      GlobalDictRegistry.getColumnDictIndexes(db, table)
    }.getOrElse(Seq.empty)
  }

  def loadDictAndEncodeRelation(relation: LogicalPlan): LogicalPlan = {
    // 1. load indexes and encode cols (if any)
    // 2. record used dict to attrToIndex
    // 3. project all cols with encoded cols
    val mapping = new OriginalToEncodedMap
    loadDictIndexes(relation).foreach { dictIndex =>
      val colRef = resolveColumn(relation, dictIndex.column).get
      val encodedColRef = resolveColumn(relation, dictIndex.encodedColumn).get
      encodedColRef.setDict(dictIndex.dict)
      mapping(colRef.exprId) = encodedColRef.toAttribute
    }
    relation.withEncodingMapping(mapping)
  }

  private def resolveColumn(plan: LogicalPlan, column: String): Option[AttributeReference] = {
    plan.schema.getFieldIndex(column).map(plan.output(_).asInstanceOf[AttributeReference])
  }


  private def updateMapping(originalExpr: NamedExpression, encodedExpr: NamedExpression, dict: ColumnDict): Unit = {
    val map = currentPlan.getEncodingMapping.getOrElse(new OriginalToEncodedMap)
    val encodedAttr = encodedExpr.toAttribute
    encodedExpr.setDict(dict)
    encodedAttr.setDict(dict) // copy dict tag to attr as well

    map.put(
      originalExpr.exprId,
      encodedAttr)
    if (map.size == 1) { // new map
      currentPlan.withEncodingMapping(map)
    }
  }

}
