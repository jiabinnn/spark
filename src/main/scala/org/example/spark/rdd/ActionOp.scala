package org.example.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class ActionOp {

  val conf = new SparkConf().setMaster("local[6]").setAppName("transformation_op")
  val sc = new SparkContext(conf)

  /**
   * 注意点：
   * 1 函数中传入的curr参数，并不是value，而是一整条数据
   * 2 reduce整体上的结果，只有一个
   */
  @Test
  def reduce(): Unit = {
    val rdd = sc.parallelize(Seq(("手机", 10.0), ("手机", 15.0), ("电脑", 20.0)))
    val result: (String, Double) = rdd.reduce((curr, agg) => ("总价：", curr._2 + agg._2))
    println(result)
  }

  @Test
  def foreach() : Unit = {
    val rdd = sc.parallelize(Seq(1,2,3))
    rdd.foreach(item => println(item))
  }

  /**
   * count 和 countByKey的结果相距很远
   * 每次调用action都会生成一个job，job会运行获取结果
   * 所以连个job之间会有大量日志打出，其实就是在启动job
   * countByKey的运算结果是Map(Key, Value->Key的count)
   */
  @Test
  def count() : Unit = {
    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("c", 3), ("d", 4)))
    println(rdd.count())
    println(rdd.countByKey())
  }

  /**
   * take 和 takeSample都是获取数据，一个直接获取，一个采样获取
   * first：一般情况下action会从所有分区获取数据，相对来说速度比较慢，
   * first只获取第一个元素，所以first只会处理第一个分区，所以速度很快，无需处理所有数据
   */
  @Test
  def take() : Unit = {
    val rdd = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)))
    rdd.take(3).foreach(println(_))
    println(rdd.first())
    rdd.takeSample(withReplacement = true, num = 3).foreach(println(_))
  }

  /**
   * 除了这四个支持以外，还有其他很多特殊的支持
   */
  @Test
  def numeric(): Unit = {
    val rdd = sc.parallelize(Seq(1,2,3,4,10,20,30,50,100))
    println(rdd.max())
    println(rdd.min())
    println(rdd.mean())
    println(rdd.sum())
  }
}
