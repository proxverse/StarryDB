package org.apache.spark.sql.columnar.plan

import org.apache.spark.SparkConf
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{JoinSuite, Row}
import org.scalactic.source.Position
import org.scalatest.Tag

import scala.collection.JavaConverters._

class ColumnarMergeJoinSuite extends JoinSuite with ColumnarSharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.sql.starry.columnar.forceShuffledHashJoin", "false")
    conf
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    val ignores = Seq(
      "join operator selection", //smg
      "broadcasted existence join operator selection",
      "cross join with broadcast",
      "test SortMergeJoin (with spill)",
      "test SortMergeJoin output ordering",
      "SPARK-22445 Respect stream-side child's needCopyResult in BroadcastHashJoin",
      "SPARK-26352: join reordering should not change the order of columns", //cross join
      "NaN and -0.0 in join keys", // normalize_nanand_zero
      "SPARK-35984: Config to force applying shuffled hash join",
      "SPARK-34593: Preserve broadcast nested loop join partitioning and ordering",
      "SPARK-32399: Full outer shuffled hash join",
      "SPARK-32383: Preserve hash join (BHJ and SHJ) stream side ordering",
      "SPARK-28323: PythonUDF should be able to use in join condition",
      "SPARK-28345: PythonUDF predicate should be able to pushdown to join",
      "SPARK-32649: Optimize BHJ/SHJ inner/semi join with empty hashed relation",
      "SPARK-32330: Preserve shuffled hash join build side partitioning")
    if (!ignores.contains(testName)) {
      super.test(testName, testTags: _*)(testFun)
    }
//    if ("big inner join, 4 matches per row2".equals(testName)) {
//      super.test(testName, testTags: _*)(testFun)
//    }
  }

  test("left outer join2") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N", "left"),
        Row(1, "A", 1, "a") ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)

      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N" && $"n" > 1, "left"),
        Row(1, "A", null, null) ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)

      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N" && $"N" > 1, "left"),
        Row(1, "A", null, null) ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)

      checkAnswer(
        upperCaseData.join(lowerCaseData, $"n" === $"N" && $"l" > $"L", "left"),
        Row(1, "A", 1, "a") ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)

      // Make sure we are choosing left.outputPartitioning as the
      // outputPartitioning for the outer join operator.
      checkAnswer(
        sql(
          """
            |SELECT l.N, count(*)
            |FROM uppercasedata l LEFT OUTER JOIN allnulls r ON (l.N = r.a)
            |GROUP BY l.N
          """.stripMargin),
        Row(
          1, 1) ::
          Row(2, 1) ::
          Row(3, 1) ::
          Row(4, 1) ::
          Row(5, 1) ::
          Row(6, 1) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT r.a, count(*)
            |FROM uppercasedata l LEFT OUTER JOIN allnulls r ON (l.N = r.a)
            |GROUP BY r.a
          """.stripMargin),
        Row(null, 6) :: Nil)
    }
  }


  test("tests") {
    spark.sharedState.cacheManager.clearCache()
    sql("CACHE TABLE testData")
    sql("CACHE TABLE testData2")
    Seq(
      ("SELECT * FROM testData LEFT JOIN testData2 ON key = a", classOf[BroadcastHashJoinExec]),
//      ("SELECT * FROM testData RIGHT JOIN testData2 ON key = a where key = 2",
//          classOf[BroadcastHashJoinExec]),
//      ("SELECT * FROM testData right join testData2 ON key = a and key = 2",
//          classOf[BroadcastHashJoinExec])
    ).foreach(assertJoin)
  }
  test("SPARK-24495: Join may return wrong result when having duplicated equal-join keys2") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = spark.range(0, 100, 1, 2)
      val df2 = spark.range(100).select($"id".as("b1"), (- $"id").as("b2"))
      val res = df1.join(df2, $"id" === $"b1" && $"id" === $"b2").select($"b1", $"b2", $"id")
      checkAnswer(res, Row(0, 0, 0))
    }
  }


  test("outer broadcast hash join should not throw NPE2") {
    withTempView("v1", "v2") {
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        Seq(2 -> 2).toDF("x", "y").createTempView("v1")

        spark.createDataFrame(
          Seq(Row(1, "a")).asJava,
          new StructType().add("i", "int", nullable = false).add("j", "string", nullable = false)
        ).createTempView("v2")

        checkAnswer(
          sql("select x, y, i, j from v1 left join v2 on x = i and y < length(j)"),
          Row(2, 2, null, null)
        )
      }
    }
  }
}
