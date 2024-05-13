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
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{arrays_zip, base64, col, hash, lit, slice}
import org.apache.spark.sql.{SQLContext, SparkSession, functions}

class RewriteWithGlobalDictSuite extends PlanTest {

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

    override def valueCount: Int = 0
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

  override def afterAll(): Unit = {
    super.afterAll()
    GlobalDictRegistry.invalidate("db", "foo")
    GlobalDictRegistry.invalidate("db", "activity")
    GlobalDictRegistry.invalidate("db", "case")
  }

//  test("test filter proj") {
//    val plan = foo
//      .where($"event".isNotNull)
//      .select($"event", $"ts")
//      .analyze
//    val dictOpt = DictOptimize.execute(plan)
//    println(dictOpt)
//  }
//
//  test("test agg") {
//    val plan = foo
//      .where($"event".isNotNull)
//      .groupBy($"ts")(count($"event"))
//      .analyze
//    val dictOpt = DictOptimize.execute(plan)
//    println(dictOpt)
//  }

  // activity --> city
  //  \----> case ---> owner
  //           \---->  supplier

  private def table(name: String): LogicalPlan = {
    name match {
      case "activity" => activityTable()
      case "case" => caseTable()
      case _ => null
    }
  }

  def assertPlan(plan: LogicalPlan, expected: LogicalPlan): Unit = {
    assert(normalizePlan(normalizeExprIds(plan)).toString()
      == normalizePlan(normalizeExprIds(expected)).toString())
  }

  test("test group by encoded col") {
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .where($"case.id" > 0)
      .groupBy($"activity.action")($"activity.action", count($"activity.id"))
      .analyze
    val optimized = DictOptimize.execute(plan)

    val expected = Optimize.execute(
      table("case")
        .join(
          table("activity")
            .select($"activity.id", $"activity.encoded_action"),
          LeftOuter,
          Some($"case.id".attr === $"activity.id".attr))
        .where($"case.id" > 0)
        .groupBy($"encoded_action")(
          LowCardDictDecode($"encoded_action", dummyDict).as("action_encoded_col_0"),
          count($"activity.id"))
        .select($"action_encoded_col_0".as("action"), $"`count(id)`")
        .analyze)

    assertPlan(optimized, expected)
  }

  test("test group by encoded col - multi table") { // only group by col should be encoded
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .join(table("activity").as("A1"), LeftOuter, Some($"A1.id".attr === $"activity.id".attr))
      .where($"case.id" > 0)
      .where("A1.action" === "open")
      .groupBy($"activity.action")($"activity.action", count($"A1.city"))
      .analyze
    val optimized = DictOptimize.execute(plan)

    val expected = Optimize.execute(
      table("case")
        .join(
          table("activity")
            .select($"activity.id", $"activity.encoded_action"),
          LeftOuter,
          Some($"case.id".attr === $"activity.id".attr))
        .join(table("activity").as("A1"), LeftOuter, Some($"A1.id".attr === $"activity.id".attr))
        .where($"case.id" > 0)
        .where("A1.action" === "open")
        .groupBy($"activity.encoded_action")(
          LowCardDictDecode($"activity.encoded_action", dummyDict).as("action"),
          count($"A1.city"))
        .select($"action", $"`count(city)`")
        .analyze)

    assertPlan(optimized, expected)
  }

  test("test agg collect_list") {
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .join(table("activity").as("A1"), LeftOuter, Some($"A1.id".attr === $"activity.id".attr))
      .where($"case.id" > 0)
      .where("A1.action" === "open")
      .groupBy($"activity.city")($"activity.city", collectList($"activity.action").as("lst"))
      .select(
        $"activity.city",
        Inline(
          arrays_zip(
            slice(col("lst"), lit(1), functions.size(col("lst")) - 1),
            slice(col("lst"), lit(2), functions.size(col("lst")) - 1)).expr))
      .analyze
    val optimized = DictOptimize.execute(plan)

    val expected = Optimize.execute(
      table("case")
        .join(
          table("activity")
            .select($"activity.id", $"activity.encoded_action", $"activity.city"),
          LeftOuter,
          Some($"case.id".attr === $"activity.id".attr))
        .join(table("activity").as("A1"), LeftOuter, Some($"A1.id".attr === $"activity.id".attr))
        .where($"case.id" > 0)
        .where("A1.action" === "open")
        .groupBy($"activity.city")(
          $"activity.city",
          collectList($"activity.encoded_action").as("lst"))
        .select(
          $"activity.city",
          Inline(
            arrays_zip(
              slice(col("lst"), lit(1), functions.size(col("lst")) - 1),
              slice(col("lst"), lit(2), functions.size(col("lst")) - 1)).expr))
        .select(
          $"activity.city",
          LowCardDictDecode($"`0`", dummyDict).as("0"),
          LowCardDictDecode($"`1`", dummyDict).as("1"))
        .analyze)

    assertPlan(optimized, expected)
  }

  test("test agg collect_list - array decode") {
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .join(table("activity").as("A1"), LeftOuter, Some($"A1.id".attr === $"activity.id".attr))
      .where($"case.id" > 0)
      .where("A1.action" === "open")
      .groupBy($"activity.action")(collectList($"activity.action"), count($"A1.city"))
      .analyze
    val optimized = DictOptimize.execute(plan)

    val expected = Optimize.execute(
      table("case")
        .join(
          table("activity")
            .select($"activity.id", $"activity.encoded_action", $"activity.city"),
          LeftOuter,
          Some($"case.id".attr === $"activity.id".attr))
        .join(table("activity").as("A1"), LeftOuter, Some($"A1.id".attr === $"activity.id".attr))
        .where($"case.id" > 0)
        .where("A1.action" === "open")
        .groupBy($"activity.encoded_action")(collectList($"activity.encoded_action"), count($"A1.city"))
        .analyze
        .select(
          LowCardDictDecodeArray($"`collect_list(encoded_action)`", dummyDict).as(
            "collect_list(action)"),
          $"`count(city)`")
        .analyze)

    assertPlan(optimized, expected)
  }

  test("test group by encoded col - with filter") {
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .where(IsNotNull($"activity.action") && ($"case.id" > 0))
      .groupBy($"activity.action")($"activity.action", count($"activity.id"))
      .analyze
    val optimized = DictOptimize.execute(plan)

    val expected = Optimize.execute(
      table("case")
        .join(
          table("activity")
            .select($"activity.id", $"activity.encoded_action"),
          LeftOuter,
          Some($"case.id".attr === $"activity.id".attr))
        .where(col("encoded_action").isNotNull.expr && ($"case.id" > 0))
        .groupBy($"encoded_action")(
          LowCardDictDecode($"encoded_action", dummyDict).as("action"),
          count($"activity.id"))
        .analyze)

    assertPlan(optimized, expected)
  }

  ignore("no encode is supposed") {
    val planProject = Optimize.execute(
      table("case")
        .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
        .where($"case.id" > 0)
        .select($"activity.action", $"activity.id", $"case.id")
        .analyze)
    assertPlan(planProject, DictOptimize.execute(planProject))

    val planAggregate = Optimize.execute(
      table("case")
        .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
        .where($"case.id" > 0)
        .groupBy($"activity.id")($"activity.id", count($"case.id"))
        .analyze)
    assertPlan(planAggregate, DictOptimize.execute(planAggregate))

    val planWindow = Optimize.execute(
      table("case")
        .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
        .where($"case.id" > 0)
        .window(
          count($"activity.id").as("cnt") :: Nil,
          $"activity.action" :: Nil,
          $"activity.id".asc :: Nil))
    assertPlan(planWindow, DictOptimize.execute(planWindow))
  }

  ignore("test filter in") {
    // 实际一样，但是id对不上
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .where($"activity.action".in("A", "B", "C", "D") && ($"case.id" > 0))
      .groupBy()(count($"activity.id"))
      .analyze
    val optimized = DictOptimize.execute(plan)

    val expected = Optimize.execute(
      table("case")
        .join(
          table("activity")
            .select($"activity.id", $"activity.id".as("action_encoded_col_0")),
          LeftOuter,
          Some($"case.id".attr === $"activity.id".attr))
        .where(
          LowCardDictDecode(
            col("action_encoded_col_0").expr,
            new ExecutionColumnDict(dummyDict, $"dict".in("A", "B", "C", "D").expr, BooleanType, "")
              .asInstanceOf[ColumnDict]) && ($"case.id" > 0))
        .groupBy()(count($"activity.id"))
        .analyze
        .analyze)

    assertPlan(optimized, expected)
  }

  ignore("test encoded column") {
    // 实际一样，但是id对不上
    val plan = table("case")
      .join(table("activity"), LeftOuter, Some($"case.id".attr === $"activity.id".attr))
      .where($"case.owner".in("A", "B"))
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
            new ExecutionColumnDict(dummyDict, $"dict".in(0, 1).expr, BooleanType, "")
              .asInstanceOf[ColumnDict]))
        .groupBy($"action_encoded_col_0")(count($"activity.id"))
        .analyze
        .analyze)

    comparePlans(optimized, expected)
  }

}
