package org.example.spark.sql

import java.lang

import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.junit.Test

class TypedTransformation {

  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("typedTransformation")
    .getOrCreate()

  import spark.implicits._

  @Test
  def trans(): Unit = {
    // flatmap
    val ds = Seq("hello spark", "hello hadoop").toDS
    ds.flatMap(item => item.split(" ")).show()

    // map
    val ds2 = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDS()
    ds2.map(person => Person(person.name, person.age * 2)).show()

    // mapPartitions
    ds2.mapPartitions(
      // iter不能大到每个分区的内存放不下，不然就会内存溢出
      // 对每个元素进行转换，后生成一个新的集合
      iter => {
        val result = iter.map(person => Person(person.name, person.age * 2))
        result
      }
    ).show()
  }

  @Test
  def trans1(): Unit ={
    val ds = spark.range(10)
    ds.transform(dataset => dataset.withColumn("doubled", 'id * 2))
      .show()
  }

  @Test
  def as(): Unit ={
    // 读取
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )

    val df: DataFrame = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv("dataset/studenttab10k")
    // 转换
    val ds: Dataset[Student] = df.as[Student]

    // 输出
    ds.show()
  }

  @Test
  def filter(): Unit ={
    val ds = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDS()
    ds.filter(item => item.age > 15).show()
  }

  @Test
  def groupByKey(): Unit ={
    val ds = Seq(Person("zhangsan", 15), Person("zhangsan", 16), Person("lisi", 20)).toDS()
    val grouped: KeyValueGroupedDataset[String, Person] = ds.groupByKey(person => person.name)
    val result: Dataset[(String, Long)] = grouped.count()
    result.show()

  }

  @Test
  def split(): Unit ={
    val ds = spark.range(15)
    // 切多少分，权重多少
    val datasets: Array[Dataset[lang.Long]] = ds.randomSplit(Array(5,2,3))
    datasets.foreach(_.show())

    //sample
    ds.sample(false, fraction = 0.4).show()
  }

  @Test
  def sort(): Unit ={
    val ds = Seq(Person("zhangsan", 15), Person("zhangsan", 16), Person("lisi", 20)).toDS()
    ds.orderBy('age.desc).show()
    ds.sort('age.asc).show()
  }

  @Test
  def dropDuplicates(): Unit ={
    val ds = Seq(Person("zhangsan", 15), Person("zhangsan", 15), Person("lisi", 15)).toDS()
    ds.distinct().show()

    ds.dropDuplicates("age").show()

  }

  @Test
  def collection(): Unit ={
    val ds1 = spark.range(1,10)
    val ds2 = spark.range(5,15)

    //差级
    ds1.except(ds2).show()

    //交集
    ds1.intersect(ds2).show()

    //并集
    ds1.union(ds2).show()

    //limit
    ds1.limit(3).show()
  }
}
case class Student(name: String, age: Int, gpa: Float)

