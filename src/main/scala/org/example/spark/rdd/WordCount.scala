package org.example.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
class WordCount {
  def main(args: Array[String]): Unit = {
    //1 创建SparkContext
    val conf = new SparkConf().setAppName("word_count").setMaster("local[6]")
    val sc = new SparkContext(conf)
    //2 加载文件
    val rdd1 = sc.textFile("dataset/wordcount.txt")
//    val rdd1 = sc.textFile("hdfs:///data/wordcount.txt")

    //3 处理
    val rdd2 = rdd1.flatMap(item => item.split(" "))
    val rdd3 = rdd2.map(item => (item, 1))
    val rdd4 = rdd3.reduceByKey((curr, agg) => curr + agg)
    //4 得到结果
    val result = rdd4.collect()
    result.foreach(item => println(item))

  }

  @Test
  def sparkContext() : Unit = {
    //1  创建SparkConf
    val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context")
    //2 创建SparkContext
    val sc = new SparkContext(conf)
  }
  //1  创建SparkConf
  val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context")
  //2 创建SparkContext
  val sc = new SparkContext(conf)
  @Test
  def rddCreationLocal():Unit = {
    // 从本地创建
    val seq = Seq(1,2,3)
    val rdd1: RDD[Int] = sc.parallelize(seq, 2)
    val rdd2: RDD[Int] = sc.makeRDD(seq, 2)
  }

  @Test
  def rddCreationFiles():Unit = {
    // 从文件创建
    val rdd = sc.textFile("dataset/wordcount.txt")
  }

  @Test
  def rddCreationFromRDD():Unit = {
    // 从RDD衍生
    val rdd1 = sc.parallelize(Seq(1,2,3))
    val rdd2: RDD[Int] = rdd1.map(item => item)
  }


  @Test
  def mapTest(): Unit = {
    val rdd1 = sc.parallelize(Seq(1,2,3))
    val rdd2 = rdd1.map(item => item * 10)
    val result = rdd2.collect()
    result.foreach(item => println(item))
  }

  @Test
  def flatMapTest(): Unit = {
    val rdd1 = sc.parallelize(Seq("hello world", "hello world", "hello spark"))
    val rdd2: RDD[String] = rdd1.flatMap(item => item.split(" "))
    val result = rdd2.collect()
    result.foreach(item => println(item))
    sc.stop()
  }

  @Test
  def reduceByKeyTest(): Unit = {
    val rdd1 = sc.parallelize(Seq("hello scala", "hello world", "hello spark"))
    val rdd2: RDD[(String, Int)] = rdd1.flatMap(item => item.split(" "))
      .map(item => (item, 1))
      .reduceByKey((curr, agg) => curr + agg)

    val result = rdd2.collect()
    result.foreach(item => println(item))
    sc.stop()
  }
}
