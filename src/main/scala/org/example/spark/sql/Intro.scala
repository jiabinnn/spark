package org.example.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

case class Person(name: String, age: Int)


class Intro {

  @Test
  def rddIntro(): Unit = {
    val conf = new SparkConf().setAppName("rddIntro").setMaster("local[6]")
    val sc = new SparkContext(conf)

    sc.textFile("dataset/wordcount.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println(_))
  }


  @Test
  def dsIntro(): Unit = {
    val spark = new SparkSession.Builder()
      .appName("dsIntro")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    val personDS = sourceRDD.toDS()

    val resultDS = personDS.where('age > 10)
      .where('age < 20)
      .select('name)
      .as[String]

    resultDS.show()
  }

  @Test
  def dfIntro(): Unit = {
    val spark = new SparkSession.Builder()
      .appName("dsIntro")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    val df = sourceRDD.toDF()
    df.createOrReplaceTempView("person")
    val resultDF = spark.sql("select name from person where age > 10 and age < 20")
    resultDF.show()
  }

  /**
   * Dataset
   */
  @Test
  def dataset1(): Unit = {
    // 创建sparksession
    val spark = new SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 演示
    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    val dataset = sourceRDD.toDS()

    // Dataset 支持强类型的(类似RDD的)API
    dataset.filter(item => item.age > 10).show()
    // Dataset 支持弱类型的API
    dataset.filter('age > 10).show()
    dataset.filter($"age" > 10).show()
    // Dataset可以直接编写SQL表达式
    dataset.filter("age > 10").show()
  }

  @Test
  def dataset2(): Unit = {
    // 创建sparksession
    val spark = new SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 演示
    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    val dataset = sourceRDD.toDS()

    // 显示优化过程
//    dataset.explain(true)

    // 无论dataset中放置的是什么类型的对象，最终执行计划中的RDD上都是InternalRow
    val excutionRdd: RDD[InternalRow] = dataset.queryExecution.toRdd

  }
  @Test
  def dataset3(): Unit = {
    // 创建sparksession
    val spark = new SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    val dataset = spark.createDataset(Seq(Person("zhangsan", 10), Person("lisi", 15)))

    // 直接获取到已经分析和解析过的dataset的执行计划，直接拿到rdd
    val excutionRdd: RDD[InternalRow] = dataset.queryExecution.toRdd

    // 将dataset底层的rdd[internalrow]通过decoder转成了和dataset一样的类型的rdd
    val typedRdd: RDD[Person] = dataset.rdd

    println(excutionRdd.toDebugString)
    println()
    println()
    println(typedRdd.toDebugString)

  }

  @Test
  def dataFrame1(): Unit ={
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("dataframe1")
      .master("local[6]")
      .getOrCreate()

    // 创建DataFrame
    /**
     * 创建DataFrame的三种方式
     * toDF
     * createDataFrame
     */
    import spark.implicits._

    val dataFrame: DataFrame = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDF()

    // 看看DataFrame可以玩出什么花样
    // select name from dataFrame where age > 10
    dataFrame.where('age > 10)
      .select('name)
      .show()
  }

  @Test
  def dataFrame2(): Unit ={
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("dataframe2")
      .master("local[6]")
      .getOrCreate()

    /**
     * 创建DataFrame的三种方式
     * toDF
     * createDataFrame
     * read
     */
    import spark.implicits._

    val personList = Seq(Person("zhangsan", 15), Person("lisi", 20))

    // toDF
    val df1 = personList.toDF()
    val df2 = spark.sparkContext.parallelize(personList).toDF()

    //createDataFrame
    val df3 = spark.createDataFrame(personList)

    //read
    val df4 = spark.read.csv("dataset/BeijingPM20100101_20151231_noheader.csv")

    df4.show()
  }

  @Test
  def dataFrame3(): Unit ={
    // 创建sparkSession
    val spark = SparkSession.builder()
      .appName("dataFrame3")
      .master("local[6]")
      .getOrCreate()

    // 读取数据集
    val sourceDF: DataFrame = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    import spark.implicits._
    // 查看DataFrame的Schema信息，要意识到DataFrame中是有结构信息的，叫做schema
//    sourceDF.printSchema()

    // 处理
    //  1 选择列
    //  2 过滤掉NA的PM记录
    //  3 分组select year, month, count(PM_dongsi) from ...
    //  where PM_dongsi != NA
    //  group by year, month
    //  聚合
//    sourceDF.select('year, 'month, 'PM_Dongsi)
//      .where('PM_Dongsi =!= "NA")
//      .groupBy('year, 'month)
//      .count()
//      .show()

    // 直接使用SQL
    //1. 将DF注册成临表
    sourceDF.createOrReplaceTempView("pm")
    //2. 执行查询
    val resultDF = spark.sql("select year, month, count(PM_Dongsi) from pm where PM_Dongsi != 'NA' group by year, month")
    resultDF.show()

    spark.stop()

  }

  @Test
  def dataFrame4(): Unit = {
    // 创建sparkSession
    val spark = SparkSession.builder()
      .appName("dataFrame3")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._
    val person = Seq(Person("zhangsan", 15), Person("lisi", 20))

    val df = person.toDF()
    df.map((row: Row) => Row(row.get(0), row.getAs[Int](1) * 2))(RowEncoder.apply(df.schema))
      .show()

    val ds = person.toDS()
    ds.map((person: Person) => Person(person.name, person.age * 2))
      .show()
  }

  @Test
  def row(): Unit ={
    // Row如何创建
    // Person有类型的，列的名字：name，age
    // Row对象中没有名字，row对象必须配合schema对象才会有列名
    val p = Person("zhangsan", 15)
    val row = Row("zhangsan", 15)

    // 如何从row中获取数据
    row.getString(0)
    row.getInt(1)

    // row也是样例类
    row match {
      case Row(name, age) => println(name, age)
    }
  }
}