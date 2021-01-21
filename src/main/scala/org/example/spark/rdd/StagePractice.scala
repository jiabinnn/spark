package org.example.spark.rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class StagePractice {

  /**
   * 练习：从数据集中求pm值最大的10天
   * 数据集：dataset/BeijingPM20100101_20151231_noheader.csv
   */
  @Test
  def pmProcess(): Unit = {
    //1 创建sc
    val conf = new SparkConf().setMaster("local[6]").setAppName("stage_practice")
    val sc = new SparkContext(conf)

    //2 读取文件
    val source = sc.textFile("dataset/BeijingPM20100101_20151231_noheader.csv")

    //3 通过算子处理数据
    // 1 抽取数据，年，月，PM 返回结果：（key: (年， 月)，value: PM ）
    // 2 清洗
    // 3 聚合
    // 4 排序
    val resultRDD = source.map(item => ((item.split(",")(1), item.split(",")(2)), item.split(",")(6)))
      // item._2 就是 value
      .filter(item => StringUtils.isNotEmpty(item._2) && !item._2.equalsIgnoreCase("NA"))
      // 把item._2转成int
      .map(item => (item._1, item._2.toInt))
      // key相同的value加起来
      .reduceByKey((curr, agg) => curr + agg)
      // 按照value排序
      .sortBy(item => item._2)

    //4 获取结果
    resultRDD.take(10).foreach(println(_))

    //5 运行测试
    sc.stop()
  }
}
