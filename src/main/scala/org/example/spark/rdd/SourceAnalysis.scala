package org.example.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class SourceAnalysis {

  @Test
  def wordCount(): Unit = {
    val conf = new SparkConf().setMaster("local[6]").setAppName("SourceAnalysis")
    val sc = new SparkContext(conf)

    val textRDD: RDD[String] = sc.parallelize(Seq("hadoop spark", "flume hadoop", "spark sqoop"))

    val splitRdd: RDD[String] = textRDD.flatMap(_.split(" "))

    val tupleRdd: RDD[(String, Int)] = splitRdd.map((_, 1))

    val reduceRdd: RDD[(String, Int)] = tupleRdd.reduceByKey(_ + _)

    val strRdd: RDD[String] = reduceRdd.map(item => s"${item._1}, ${item._2}")

    //    strRdd.collect().foreach(println(_))
    println(strRdd.toDebugString)
  }

  @Test
  def narrowDependency(): Unit = {
    val conf = new SparkConf().setMaster("local[6]").setAppName("cartesian")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Seq(1,2,3,4,5,6))
    val rdd2 = sc.parallelize(Seq("a", "b", "c"))

    val resultRdd = rdd1.cartesian(rdd2)
    resultRdd.collect().foreach(println(_))
    sc.stop()
  }
}
