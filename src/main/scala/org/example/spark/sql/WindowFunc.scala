package org.example.spark.sql

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.junit.Test

class WindowFunc {
  @Test
  def top2(): Unit = {
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

    /**
     * 需求：
     * 从数据集中得到每个类别收入第一的商品和收入第二的商品
     * 关键点是, 每个类别, 收入前两名
     */

    /**
     * 1. 按照类别分组
     * 2. 每个类别中的数据按照收入排序
     * 3. 为排序过的数据增加编号
     * 4. 取得每个类别中的前两个数据作为最终结果
     */


    // 1. 定义窗口
    val window = Window.partitionBy($"category")
      .orderBy($"revenue".desc)

    // 2. 数据处理
    import org.apache.spark.sql.functions._
    source.select($"product", $"category", $"revenue", dense_rank() over window as "rank")
      .where($"rank" <= 2)
      .show()

    /**
     * +----------+----------+-------+----+
     * |   product|  category|revenue|rank|
     * +----------+----------+-------+----+
     * |      Thin|Cell phone|   6000|   1| 并列第一
     * | Very thin|Cell phone|   6000|   1| 并列第一
     * |Ultra thin|Cell phone|   5000|   2|
     * |      Pro2|    Tablet|   6500|   1|
     * |      Mini|    Tablet|   5500|   2|
     * +----------+----------+-------+----+
     */

    source.createOrReplaceTempView("productRevenue")

    spark.sql("select product, category, revenue "+
    "from (select *, dense_rank() over (partition by category order by revenue desc) as rank from productRevenue) " +
      "where rank <= 2")
      .show()

    /**
     * +----------+----------+-------+
     * |   product|  category|revenue|
     * +----------+----------+-------+
     * |      Thin|Cell phone|   6000|
     * | Very thin|Cell phone|   6000|
     * |Ultra thin|Cell phone|   5000|
     * |      Pro2|    Tablet|   6500|
     * |      Mini|    Tablet|   5500|
     * +----------+----------+-------+
     */
  }

  @Test
  def best_last(): Unit ={
    /**
     * 最优差值案例
     */

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

    val window = Window.partitionBy($"category")
      .orderBy($"revenue".desc)
    import org.apache.spark.sql.functions._
    val maxPrice: sql.Column = max($"revenue") over window

    source.select($"product", $"category", $"revenue", (maxPrice - $"revenue") as "revenue_difference")
      .show()
    /**
     * +----------+----------+-------+------------------+
     * |   product|  category|revenue|revenue_difference|
     * +----------+----------+-------+------------------+
     * |      Thin|Cell phone|   6000|                 0|
     * | Very thin|Cell phone|   6000|                 0|
     * |Ultra thin|Cell phone|   5000|              1000|
     * |  Bendable|Cell phone|   3000|              3000|
     * |  Foldable|Cell phone|   3000|              3000|
     * |      Pro2|    Tablet|   6500|                 0|
     * |      Mini|    Tablet|   5500|              1000|
     * |       Pro|    Tablet|   4500|              2000|
     * |       Big|    Tablet|   2500|              4000|
     * |    Normal|    Tablet|   1500|              5000|
     * +----------+----------+-------+------------------+
     */
  }
}
