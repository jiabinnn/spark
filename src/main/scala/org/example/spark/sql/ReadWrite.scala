package org.example.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, SaveMode, SparkSession}
import org.junit.Test

class ReadWrite {

  @Test
  def reader1(): Unit ={
    // 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    // 框架在哪
    val reader: DataFrameReader = spark.read


  }

  /**
   * 初体验Reader
   */
  @Test
  def reader2(): Unit ={
    // 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    spark.read
      .format("csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .load("dataset/BeijingPM20100101_20151231.csv")
      .show(10)

    spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")
      .show()
  }


  @Test
  def writer1(): Unit = {
    // 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()


    val df = spark.read.option("header", value = true).csv("dataset/BeijingPM20100101_20151231.csv")
    df.write.json("dataset/mybeijing_pm.json")
    df.write.format("json").save("dataset/mybeijing_pm2.json")
  }

  @Test
  def parquet(): Unit ={
    // 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    // 读取csv文件的数据
    val df = spark.read.option("header", true).csv("dataset/BeijingPM20100101_20151231.csv")

    // 把数据写为parquet格式
    // 默认写入模式就是parquet
    // 写入模式 报错，覆盖，追加，忽略
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)// 文件已存在的话重写
      .save("dataset/mybeijing_pm3")

    // 读取parquet格式文件
    // 默认读取格式是parquet
    spark.read
      .load("dataset/mybeijing_pm3")
      .show()
  }

  @Test
  def parquetPartitions(): Unit ={
    // 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

//    val df = spark.read
//      .option("header", value = true)
//      .csv("dataset/BeijingPM20100101_20151231.csv")
//
//    // 写文件，表分区
//    df.write
//      .partitionBy("year", "month")
//      .save("dataset/mybeijing_pm4")

    // 读文件，自动发现分区
    // 写分区表的时候，分区列不会包含在生成的文件中
    // 直接通过文件来进行读取的话，分区信息会丢失
    spark.read
//      .parquet("dataset/mybeijing_pm4/year=2010/month=1")
      .parquet("dataset/mybeijing_pm4")
      .printSchema()
  }

  @Test
  def json(): Unit ={
    // 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    df.write.json("dataset/mybeijing_pm5.json")
  }

  /**
   * toJSON的场景
   * 处理完了以后，DataFrame中如果是一个对象，如果其他的系统只支持JSON格式的数据
   * SparkSQL如果和这种系统进行整合的时候，就需要进行转换
   */
  @Test
  def json1(): Unit ={
    // 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    df.toJSON.show()

  }

  @Test
  def json2(): Unit ={
    // 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    val jsonRDD: RDD[String] = df.toJSON.rdd
    spark.read.json(jsonRDD).show()
  }
}
