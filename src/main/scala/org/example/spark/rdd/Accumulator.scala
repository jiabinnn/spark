package org.example.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2
import org.junit.Test

import scala.collection.mutable

class Accumulator {
  @Test
  def acc(): Unit = {
    val conf = new SparkConf().setAppName("acc").setMaster("local[6]")
    val sc = new SparkContext(conf)

    val numacc = new NumAccumulator()

    sc.register(numacc, "num")

    sc.parallelize(Seq("1", "2", "3"))
      .foreach(item => numacc.add(item))
    println(numacc.value)
  }

}

class NumAccumulator extends AccumulatorV2[String, Set[String]]{
  private val nums: mutable.Set[String] = mutable.Set()

  override def isZero: Boolean = {
    nums.isEmpty
  }

  override def copy(): AccumulatorV2[String, Set[String]] = {
    val newAccumulator = new NumAccumulator()
    nums.synchronized{
      newAccumulator.nums ++= this.nums
    }
    newAccumulator
  }

  override def reset(): Unit = {
    nums.clear()
  }

  override def add(v: String): Unit = {
    nums += v
  }

  override def merge(other: AccumulatorV2[String, Set[String]]): Unit = {
    nums ++= other.value
  }

  override def value: Set[String] = {
    nums.toSet
  }
}