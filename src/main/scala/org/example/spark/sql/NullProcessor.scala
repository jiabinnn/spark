package org.example.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.junit.Test

class NullProcessor {
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("null processor")
    .getOrCreate()

  import spark.implicits._

  @Test
  def nullAndNaN(): Unit ={
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
      .option("header", value = true)
      .schema(schema)
//      .option("inferschema", value = true)  // 自动推断schema类型
      .csv("dataset/beijingpm_with_nan.csv")

    sourceDF.na.drop("any").show() // 只要有一个NaN 就丢弃
    sourceDF.na.drop("all").show() // 所有数据都是NaN的行才丢弃
    sourceDF.na.drop("any", List("year", "month", "day", "hour")).show()// 某些列的规则,只作用于List内的列

    // 填充
    sourceDF.na.fill(0).show()
    sourceDF.na.fill(0, List("year", "month")).show()
  }

  /**
   * 字符串缺失处理
   */
  @Test
  def strProcessor(): Unit ={
    val source = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")
    import spark.implicits._

    source.show()

    // 丢弃
    source.where($"PM_Dongsi" =!= "NA").show()

    // 替换
    // select name, age, case
    // when ... then ...
    // when ... then ...
    // else
    import org.apache.spark.sql.functions._
    source.select(
      'No as "id", 'year, 'month, 'day, 'hour, 'season,
      when('PM_Dongsi === "NA", Double.NaN)
        .otherwise('PM_Dongsi cast DoubleType)
        .as("pm")
    ).show()

    /**
     * +---+----+-----+---+----+------+---+
     * | id|year|month|day|hour|season| pm|
     * +---+----+-----+---+----+------+---+
     * |  1|2010|    1|  1|   0|     4|NaN|
     * |  2|2010|    1|  1|   1|     4|NaN|
     * |  3|2010|    1|  1|   2|     4|NaN|
     * |  4|2010|    1|  1|   3|     4|NaN|
     * |  5|2010|    1|  1|   4|     4|NaN|
     * |  6|2010|    1|  1|   5|     4|NaN|
     * |  7|2010|    1|  1|   6|     4|NaN|
     * |  8|2010|    1|  1|   7|     4|NaN|
     * |  9|2010|    1|  1|   8|     4|NaN|
     * +---+----+-----+---+----+------+---+
     */
  }
}
