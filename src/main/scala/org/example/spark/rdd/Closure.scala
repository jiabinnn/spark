package org.example.spark.rdd

import org.junit.Test

/**
 * 闭包
 */
class Closure {

  @Test
  def test(): Unit = {
    val f: Int => Double = closure()
    val area = f(5)
    println(area)
  }

  def closure(): Int => Double ={
    val factor = 3.14
    val areaFunction = (r: Int) => math.pow(r,2) * factor
    areaFunction
  }
}
