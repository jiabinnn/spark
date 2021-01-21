package org.example.spark.sql

import org.apache.spark.sql.SparkSession
import org.junit.Test

class UDF {

  @Test
  def firstSecond(): Unit = {
    val spark = SparkSession.builder()
      .appName("window")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("Thin", "Cell phone", 6000),
      ("Normal", "Tablet", 1500),
      ("Mini", "Tablet", 5500),
      ("Ultra thin", "Cell phone", 5000),
      ("Very thin", "Cell phone", 6000),
      ("Big", "Tablet", 2500),
      ("Bendable", "Cell phone", 3000),
      ("Foldable", "Cell phone", 3000),
      ("Pro", "Tablet", 4500),
      ("Pro2", "Tablet", 6500)
    )

    val source = data.toDF("product", "category", "revenue")

    // 聚合每个类别的总价
    import org.apache.spark.sql.functions._
    source.groupBy("category")
      .agg(sum($"revenue"))
      .show()

    /**
     * +----------+------------+
     * |  category|sum(revenue)|
     * +----------+------------+
     * |Cell phone|       23000|
     * |    Tablet|       20500|
     * +----------+------------+
     */

    // 把名称变为小写
    source.select(lower($"product"))
      .show()
    // 把价格变为字符串形式
    // 6000 => 6K
    // UDF UserDefinedFunction
    val toStrUDF = udf(toStr _)
    source.select($"product", $"category", toStrUDF($"revenue"))
      .show()

    /**
     * +----------+----------+------------+
     * |   product|  category|UDF(revenue)|
     * +----------+----------+------------+
     * |      Thin|Cell phone|          6K|
     * |    Normal|    Tablet|          1K|
     * |      Mini|    Tablet|          5K|
     * |Ultra thin|Cell phone|          5K|
     * | Very thin|Cell phone|          6K|
     * |       Big|    Tablet|          2K|
     * |  Bendable|Cell phone|          3K|
     * |  Foldable|Cell phone|          3K|
     * |       Pro|    Tablet|          4K|
     * |      Pro2|    Tablet|          6K|
     * +----------+----------+------------+
     */
  }

  def toStr(revenue: Long): String = {
    (revenue / 1000) + "K"
  }
}
