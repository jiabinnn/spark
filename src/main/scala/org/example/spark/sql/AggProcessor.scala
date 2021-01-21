package org.example.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.junit.Test

class AggProcessor {

  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("agg processpr")
    .getOrCreate()

  import spark.implicits._

  @Test
  def groupBy(): Unit ={
    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val sourceDF = spark.read
      .schema(schema)
      .option("header", value = true)
      .csv("dataset/beijingpm_with_nan.csv")

    val cleanDF = sourceDF.where($"pm" =!= Double.NaN)

    // 需求：统计每个月PM值的平均数
    // 1. 每个月的PM值-> 分组
    // 2. 这些PM值的均数
    import org.apache.spark.sql.functions._
    val groupedDF = cleanDF.groupBy($"year", $"month")
    //select avg(pm) from ... group by ...
    groupedDF.agg(avg($"pm") as "pm_avg")
      .orderBy($"pm_avg".desc)
      .show()

    /**
     * +----+-----+------------------+
     * |year|month|            pm_avg|
     * +----+-----+------------------+
     * |2013|    1|182.92816091954023|
     * |2015|   12| 162.7363387978142|
     * |2014|    2|145.76424050632912|
     * |2015|   11|127.21529745042493|
     * |2014|   10|116.90612244897959|
     * |2013|    2|111.05772230889235|
     * |2013|    3|109.90027700831025|
     * |2013|    6|109.30342577487765|
     * |2015|    2|108.68018018018019|
     * +----+-----+------------------+
     */

    groupedDF.avg("pm")
      .select($"avg(pm)" as "pm_avg")
      .orderBy("pm_avg")
      .show()


  }

  /**
   * 多维聚合
   */
  @Test
  def multiAgg(): Unit ={
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    import org.apache.spark.sql.functions._

    //不同年份，不同来源PM值的平均数
    val groupPostAndYear = pmFinal.groupBy('source, 'year)
      .agg(avg("pm") as "pm")

    //不同来源PM值的平均数
    val groupPost = pmFinal.groupBy('source)
      .agg(sum("pm") as "pm")
      .select('source, lit(null) as "year", 'pm)

    // 合并到同一个结果集中
    groupPostAndYear.union(groupPost)
      .sort('source, 'year asc_nulls_last, 'pm)
      .show()

    /**
     * asc_nulls_last ：null 放在后面
     * +-------+----+------------------+
     * | source|year|                pm|
     * +-------+----+------------------+
     * | dongsi|2013|  93.2090724784592|
     * | dongsi|2014| 87.08640822045773|
     * | dongsi|2015|  87.4922056770591|
     * | dongsi|null|         2233497.0| *
     * |us_post|2010|104.04572982326042|
     * |us_post|2011|  99.0932403834184|
     * |us_post|2012| 90.53876763535511|
     * |us_post|2013|101.71110855035722|
     * |us_post|2014| 97.73409537004964|
     * |us_post|2015| 82.78472946356158|
     * |us_post|null|         4832327.0| *
     * +-------+----+------------------+
     */
  }

  @Test
  def rollup(): Unit ={
    import org.apache.spark.sql.functions._

    val sales = Seq(
      ("Beijing", 2016, 100),
      ("Beijing", 2017, 200),
      ("Shanghai", 2015, 50),
      ("Shanghai", 2016, 150),
      ("Guangzhou", 2017, 50)
    ).toDF("city", "year", "amount")

    // 1. groupBy city, year
    // 2. groupBy city
    // 3. groupBy 全局
    sales.rollup($"city", $"year")
      .agg(sum($"amount") as "amount")
      .sort($"city".asc_nulls_last, $"year".asc_nulls_last)
      .show()

    /**
     * +---------+----+------+
     * |     city|year|amount|
     * +---------+----+------+
     * |  Beijing|2016|   100|  city, year
     * |  Beijing|2017|   200|  city, year
     * |  Beijing|null|   300|  city
     * |Guangzhou|2017|    50|  city, year
     * |Guangzhou|null|    50|  city
     * | Shanghai|2015|    50|  city, year
     * | Shanghai|2016|   150|  city, year
     * | Shanghai|null|   200|  city
     * |     null|null|   550|  全局聚合
     * +---------+----+------+
     */
  }

  @Test
  def rollup1(): Unit ={
    //1. 数据集读取
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    import org.apache.spark.sql.functions._

    //2. 聚合和统计
    // 每个PM值计量者，每年PM值统计的平均值 groupby source year
    // 每个PM值计量者，整体上的PM值统计的平均值 groupby source
    // 全局所有的计量者，和日期的PM值统计的平均值 groupby 全局
    pmFinal.rollup($"source", $"year")
      .agg(avg($"pm") as "pm")
      .sort($"source".asc_nulls_last, $"year".asc_nulls_last)
      .show()

    /**
     * +-------+----+------------------+
     * | source|year|                pm|
     * +-------+----+------------------+
     * | dongsi|2013|  93.2090724784592|
     * | dongsi|2014| 87.08640822045773|
     * | dongsi|2015|  87.4922056770591|
     * | dongsi|null| 89.15443876736389|
     * |us_post|2010|104.04572982326042|
     * |us_post|2011|  99.0932403834184|
     * |us_post|2012| 90.53876763535511|
     * |us_post|2013|101.71110855035722|
     * |us_post|2014| 97.73409537004964|
     * |us_post|2015| 82.78472946356158|
     * |us_post|null| 95.90424117331851|
     * |   null|null| 93.66274738530468|
     * +-------+----+------------------+
     */
  }

  @Test
  def cube(): Unit ={
    /**
     * cube 的功能和 rollup 是一样的, 但也有区别, 区别如下
     * rollup(A, B).sum©
     * 其结果集中会有三种数据形式: A B C, A null C, null null C
     * 结果集中没有对 B 列的聚合结果
     *
     * cube(A, B).sum©
     * 其结果集中会有四种数据形式: A B C, A null C, null null C, null B C
     * 比 rollup 的结果集中多了一个 null B C
     *
     * 也就是说, rollup 只会按照第一个列来进行组合聚合,
     * 但是 cube 会将全部列组合聚合
     */
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )
    import org.apache.spark.sql.functions._

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    pmFinal.cube($"source", $"year")
      .agg(avg($"pm") as "pm")
      .sort($"source".asc_nulls_last, $"year".asc_nulls_last)
      .show()

    /**
     * +-------+----+------------------+
     * | source|year|                pm|
     * +-------+----+------------------+ groupby
     * | dongsi|2013|  93.2090724784592|    source, year
     * | dongsi|2014| 87.08640822045773|    source, year
     * | dongsi|2015|  87.4922056770591|    source, year
     * | dongsi|null| 89.15443876736389|    source
     * |us_post|2010|104.04572982326042|    source, year
     * |us_post|2011|  99.0932403834184|    source, year
     * |us_post|2012| 90.53876763535511|    source, year
     * |us_post|2013|101.71110855035722|    source, year
     * |us_post|2014| 97.73409537004964|    source, year
     * |us_post|2015| 82.78472946356158|    source, year
     * |us_post|null| 95.90424117331851|    source
     * |   null|2010|104.04572982326042|            year
     * |   null|2011|  99.0932403834184|            year
     * |   null|2012| 90.53876763535511|            year
     * |   null|2013| 97.66173808086904|            year
     * |   null|2014| 92.44023222060957|            year
     * |   null|2015| 85.13368549370175|            year
     * |   null|null| 93.66274738530468|  全局
     * +-------+----+------------------+
     */
  }

  @Test
  def cubeSQL(): Unit ={
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )
    import org.apache.spark.sql.functions._

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    pmFinal.createOrReplaceTempView("pm_final")

    spark.sql(
      "select source, year, avg(pm) as pm from pm_final group by source, year " +
      "grouping sets((source, year), (source), (year), ()) " +
      "order by source asc nulls last, year asc nulls last")
      .show()

    /**
     * +-------+----+------------------+
     * | source|year|                pm|
     * +-------+----+------------------+
     * | dongsi|2013|  93.2090724784592|
     * | dongsi|2014| 87.08640822045773|
     * | dongsi|2015|  87.4922056770591|
     * | dongsi|null| 89.15443876736389|
     * |us_post|2010|104.04572982326042|
     * |us_post|2011|  99.0932403834184|
     * |us_post|2012| 90.53876763535511|
     * |us_post|2013|101.71110855035722|
     * |us_post|2014| 97.73409537004964|
     * |us_post|2015| 82.78472946356158|
     * |us_post|null| 95.90424117331851|
     * |   null|2010|104.04572982326042|
     * |   null|2011|  99.0932403834184|
     * |   null|2012| 90.53876763535511|
     * |   null|2013| 97.66173808086904|
     * |   null|2014| 92.44023222060957|
     * |   null|2015| 85.13368549370175|
     * |   null|null| 93.66274738530468|
     * +-------+----+------------------+
     */
  }

}
