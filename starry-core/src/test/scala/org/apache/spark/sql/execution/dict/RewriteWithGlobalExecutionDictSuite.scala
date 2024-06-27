package org.apache.spark.sql.execution.dict

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{If, Inline, IsNotNull, RLike}
import org.apache.spark.sql.catalyst.optimizer.{CollapseProject, ColumnPruning}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.StarryContext
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{arrays_zip, base64, col, hash, lit, slice}
import org.apache.spark.sql.{SQLContext, SparkSession, functions}

class RewriteWithGlobalExecutionDictSuite extends ColumnarSharedSparkSession {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Optimizer Batch", FixedPoint(5), ColumnPruning, CollapseProject) :: Nil
  }

  object DictOptimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch(
        "Optimizer Batch",
        FixedPoint(5),
        RewriteWithGlobalDict,
        ColumnPruning,
        CollapseProject) :: Nil
  }

  case class DummyRelation(_schema: StructType) extends BaseRelation {
    override def sqlContext: SQLContext = SparkSession.getActiveSession.get.sqlContext

    override def schema: StructType = _schema
  }

  case class DummyDict() extends ColumnDict {
    override def cleanup(): Unit = {}

    override def broadcastID: Long = 0

    override def broadcastNumBlocks: Int = 0
  }

  def table(db: String, table: String, schema: StructType): LogicalPlan = {
    SubqueryAlias(
      table,
      LogicalRelation(
        DummyRelation(schema),
        CatalogTable(
          TableIdentifier(table, Some(db)),
          CatalogTableType.VIEW,
          CatalogStorageFormat.empty,
          schema))
    )
  }

  val foo = () => table("db", "foo", StructType(
    StructField("id", IntegerType) ::
      StructField("event", StringType) ::
      StructField("ts", TimestampType) ::
      StructField("encoded_event", IntegerType) :: Nil
  ))
  val activityTable = () => table("db", "activity",
    StructType(Array(
      StructField("id", IntegerType),
      StructField("action", StringType),
      StructField("date", StringType),
      StructField("city", StringType),
      StructField("encoded_action", IntegerType)
    ))
  )
  val caseTable = () => table("db", "case",
    StructType(Array(
      StructField("id", IntegerType),
      StructField("owner", StringType),
      StructField("supplier", StringType),
      StructField("status", StringType),
      StructField("encoded_owner", IntegerType)
    ))
  )

  val dummyDict = DummyDict()
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    GlobalDictRegistry.register(
      "db", "foo",
      SimpleColumnDictIndex("foo", "event", "encoded_event", dummyDict))

    GlobalDictRegistry.register(
      "db", "activity",
      SimpleColumnDictIndex("activity", "action", "encoded_action", dummyDict))

    GlobalDictRegistry.register(
      "db", "case",
      SimpleColumnDictIndex("foo", "owner", "encoded_owner", dummyDict))
  }

  override protected def beforeEach(): Unit = {
    StarryContext.startNewContext()
  }

  override protected def afterEach(): Unit = {
    StarryContext.clean()
  }

  private def table(name: String): LogicalPlan = {
    name match {
      case "activity" => activityTable()
      case "case" => caseTable()
      case _ => null
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    GlobalDictRegistry.invalidate("db", "foo")
    GlobalDictRegistry.invalidate("db", "activity")
    GlobalDictRegistry.invalidate("db", "case")
  }


  // TODO fix filter with isNull/isNotNull
  ignore("test eval encoded bool expr") {
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .where(($"case.owner" === "A" && $"case.owner".isNotNull) || $"activity.action" === "A")
      .groupBy($"activity.action")(count($"activity.id"))
      .analyze
    val optimized = DictOptimize.execute(plan)

    val expected = Optimize.execute(
      table("case")
        .select($"case.id", $"case.id".as("owner_encoded_col_0"))
        .join(
          table("activity")
            .select($"activity.id", $"activity.id".as("action_encoded_col_0"),
            ),
          LeftOuter,
          Some($"case.id".attr === $"activity.id".attr))
        .where(
          LowCardDictDecode(
            col("owner_encoded_col_0").expr,
            new ExecutionColumnDict(dummyDict, ($"dict" === "A") && $"dict".isNotNull.expr, BooleanType, "")
              .asInstanceOf[SimpleColumnDict]) ||
            LowCardDictDecode(
              col("action_encoded_col_0").expr,
              new ExecutionColumnDict(dummyDict, ($"dict" === "A").expr, BooleanType, "")
                .asInstanceOf[SimpleColumnDict]))
        .groupBy($"action_encoded_col_0")(count($"activity.id"))
        .analyze
        .analyze)

    comparePlans(optimized, expected)
  }

  test("test eval encoded string expr") {
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .where($"case.owner" === "A")
      .select(
        If(
          $"activity.action" === "A",
          functions.concat(col("activity.action"), lit("0")).expr,
          lit(null).expr).as("action1"))
      .groupBy($"action1")($"action1", count($"action1"))
      .analyze
    val optimized = DictOptimize.execute(plan)

    assert(optimized.isInstanceOf[Aggregate])
    val execDict = optimized.expressions(1).find(_.isInstanceOf[LowCardDictDecode])
      .get.asInstanceOf[LowCardDictDecode].dict
    assert(execDict.isInstanceOf[ExecutionColumnDict])
  }

  test("test eval encoded unsupported expr") {
    val plan = table("activity")
      .select(hash(col("action")).as("activity").expr)
      .analyze
    val optimized = DictOptimize.execute(plan)
    assert(optimized.isInstanceOf[Project])
    assert(
      !optimized
        .asInstanceOf[Project]
        .projectList
        .exists(
          _.exists(
            e =>
              e.isInstanceOf[LowCardDictDecode] && e
                .asInstanceOf[LowCardDictDecode]
                .dict
                .isInstanceOf[ExecutionColumnDict])))
  }

  test("test eval encoded unsupported expr on filter") {
    val plan = table("activity")
      .where(base64(col("action")).isNotNull.expr)
      .analyze
    val optimized = DictOptimize.execute(plan)
    assert(optimized.isInstanceOf[Filter])
    assert(!optimized.expressions.exists(_.isInstanceOf[LowCardDictDecode]))
  }

  test("test group by encoded string expr") {
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .where($"case.owner" === "A")
      .select(
        If(
          $"activity.action" === "A",
          functions.concat(col("activity.action"), lit("0")).expr,
          lit(null).expr).as("action1"))
      .groupBy($"action1")($"action1", count($"action1").as("cnt"))
      .select($"action1".as("DIM1"), $"cnt".as("DIM2"))
      .analyze
    val optimized = DictOptimize.execute(plan)

    val execDict = optimized.expressions(1).find(_.isInstanceOf[LowCardDictDecode])
      .get.asInstanceOf[LowCardDictDecode].dict
    assert(execDict.isInstanceOf[ExecutionColumnDict])
//    assert(plan.outputSet.map(_.name) == optimized.outputSet.map(_.name))
//    assert(plan.outputSet.map(_.exprId) == optimized.outputSet.map(_.exprId))
//
//    val agg = plan.find(_.isInstanceOf[Aggregate]).get.asInstanceOf[Aggregate]
//    val optAgg = optimized.find(_.isInstanceOf[Aggregate]).get.asInstanceOf[Aggregate]
//
//    assert(agg.groupingExpressions == optAgg.groupingExpressions)
  }


}
