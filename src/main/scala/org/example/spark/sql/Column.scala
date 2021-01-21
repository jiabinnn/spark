package org.example.spark.sql

import org.apache.spark.sql
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.junit.Test

/**
 * 两种：有绑定，无绑定
 */
class Column {
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("column")
    .getOrCreate()

  import spark.implicits._

  import org.apache.spark.sql.functions._

  @Test
  def creation(): Unit ={
    val ds = Seq(Person("zhangsan", 15), Person("lisi", 10), Person("zhangsan", 15)).toDS
    val df = Seq(("zhangsan", 15), ("lisi", 10), ("zhangsan", 15)).toDF("name", "age")

    // ' 必须导入spark的隐式转换，才能使用str.intern()
    val column: Symbol = 'name  //Symbol可以理解为String

    // $ 必须导入spark的隐式转换
    val column1: ColumnName = $"name"

    // col 必须导入functions
    val column2: sql.Column = col("name")

    // column 必须导入functions
    val column3: sql.Column = column("name")

    // dataset, dataframe 可以使用column对象选中对应的列
    ds.select(column).show()
    df.select(column).show()

    /**
     * +--------+
     * |    name|
     * +--------+
     * |zhangsan|
     * |    lisi|
     * |zhangsan|
     * +--------+
     */

    // dataset.col
    val ds1 = Seq(Person("zhangsan", 15), Person("lisi", 10), Person("zhangsan", 15)).toDS
    val column4: sql.Column = ds.col("name")
    val column5: sql.Column = ds1.col("name")

    // column5 和ds1 绑定，不能在ds选择
    // 会报错
    // ds.select(column5).show()

    // 为什么要和dataset 绑定
    //ds.join(ds1, ds.col("name") === ds1.col("name"))

    //dataset.apply
    val column6: sql.Column = ds.apply("name")
    val column7: sql.Column = ds("name")
  }

  @Test
  def as(): Unit ={
    val ds = Seq(Person("zhangsan", 15), Person("lisi", 10), Person("zhangsan", 15)).toDS

    // select name, count(age) "as" age from table group by name
    ds.select('name as "new_name").show()
    /**
     * +--------+
     * |new_name|
     * +--------+
     * |zhangsan|
     * |    lisi|
     * |zhangsan|
     * +--------+
     */

    ds.select('age.as[Int]).show()
  }

  @Test
  def api(): Unit ={
    val ds = Seq(Person("zhangsan", 15), Person("lisi", 10), Person("zhangsan", 15)).toDS

    // ds 增加列，双倍年龄
    ds.withColumn("doubled", $"age" * 2).show()

    /**
     * +--------+---+-------+
     * |    name|age|doubled|
     * +--------+---+-------+
     * |zhangsan| 15|     30|
     * |    lisi| 10|     20|
     * |zhangsan| 15|     30|
     * +--------+---+-------+
     */
    // 模糊查询
    ds.where( 'name like "zhang%").show()

    /**
     * +--------+---+
     * |    name|age|
     * +--------+---+
     * |zhangsan| 15|
     * |zhangsan| 15|
     * +--------+---+
     */
    // 排序
    ds.sort($"age".desc).show()

    /**
     * +--------+---+
     * |    name|age|
     * +--------+---+
     * |zhangsan| 15|
     * |zhangsan| 15|
     * |    lisi| 10|
     * +--------+---+
     */
    // 枚举判断
    ds.where($"name" isin ("zhangsan", "wangwu", "zhaoliu")).show()

    /**
     * +--------+---+
     * |    name|age|
     * +--------+---+
     * |zhangsan| 15|
     * |zhangsan| 15|
     * +--------+---+
     */
  }
}
