package org.example.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class BroadCast {
  @Test
  def bc1(): Unit ={
    // 数据，假设这个数据很大，大概一百兆
    val v = Map("Spark" -> "http://spark.apache.cn", "Scala" -> "http://www.scala-lang.org")

    val conf = new SparkConf().setMaster("local[6]").setAppName("bc")
    val sc = new SparkContext(conf)

    // 创建广播
    val bc = sc.broadcast(v)

    val r = sc.parallelize(Seq("Spark", "Scala"))

    val result = r.map(item => bc.value(item)).collect()
    result.foreach(println(_))
  }
}
