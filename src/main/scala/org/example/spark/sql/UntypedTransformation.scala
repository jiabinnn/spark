package org.example.spark.sql

import org.apache.spark.sql.SparkSession
import org.junit.Test

class UntypedTransformation {

  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("untypedTransformation")
    .getOrCreate()

  import spark.implicits._

  /**
   * 选择列
   */
  @Test
  def select(): Unit ={
    val ds = Seq(Person("zhangsan", 12), Person("lisi", 18), Person("zhangsan", 15)).toDS

    ds.select('name).show()

    ds.selectExpr("sum(age)").show()

    import org.apache.spark.sql.functions._

    ds.select(expr("avg(age)")).show()
  }

  /**
   * 新建列
   */
  @Test
  def column(): Unit ={
    val ds = Seq(Person("zhangsan", 12), Person("lisi", 18), Person("zhangsan", 15)).toDS

    import org.apache.spark.sql.functions._
    // select rand() from ...
    // 如果想使用函数功能
    //1 使用functions.xx
    //2 使用表达式，可以使用expr("...") 随时随地编写表达式
    ds.withColumn("newColumn", expr("rand()")).show()
    /**
     * +--------+---+-------------------+
     * |    name|age|          newColumn|
     * +--------+---+-------------------+
     * |zhangsan| 12| 0.1093620429313582|
     * |    lisi| 18|0.49978073748735097|
     * |zhangsan| 15| 0.5100373615177919|
     * +--------+---+-------------------+
     */

    ds.withColumn("new_name", 'name).show()

    /**
     * +--------+---+--------+
     * |    name|age|new_name|
     * +--------+---+--------+
     * |zhangsan| 12|zhangsan|
     * |    lisi| 18|    lisi|
     * |zhangsan| 15|zhangsan|
     * +--------+---+--------+
     */

    ds.withColumn("name_joke", 'name === "zhangsan").show()

    /**
     * +--------+---+---------+
     * |    name|age|name_joke|
     * +--------+---+---------+
     * |zhangsan| 12|     true|
     * |    lisi| 18|    false|
     * |zhangsan| 15|     true|
     * +--------+---+---------+
     */
    ds.withColumnRenamed("name", "new_name").show()

    /**
     * +--------+---+
     * |new_name|age|
     * +--------+---+
     * |zhangsan| 12|
     * |    lisi| 18|
     * |zhangsan| 15|
     * +--------+---+
     */
  }


  /**
   * 剪除
   */
  @Test
  def drop(): Unit ={
    val ds = Seq(Person("zhangsan", 12), Person("lisi", 18), Person("zhangsan", 15)).toDS

    ds.drop("age").show()

    /**
     * +--------+
     * |    name|
     * +--------+
     * |zhangsan|
     * |    lisi|
     * |zhangsan|
     * +--------+
     */
  }

  /**
   * 聚合
   */
  @Test
  def groupBy(): Unit ={
    val ds = Seq(Person("zhangsan", 12), Person("lisi", 18), Person("zhangsan", 15)).toDS

    import org.apache.spark.sql.functions._
    // 为什么GroupByKey是有类型的，
    // 最主要的原因是因为groupbykey所生成的对象中的算子是有类型的
//    ds.groupByKey(_.name)

    // 为什么GroupBy是无类型的
    // 因为groupby所生成的对象中的算子是无类型的
    ds.groupBy('name).agg(mean("age")).show()

    /**
     * +--------+--------+
     * |    name|avg(age)|
     * +--------+--------+
     * |zhangsan|    13.5|
     * |    lisi|    18.0|
     * +--------+--------+
     */
  }

}
