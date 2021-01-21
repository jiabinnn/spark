package org.example.spark.rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * 缓存
 */
class CacheOp {
  @Test
  def cache():Unit = {
    /**
     * 在代码中, 多次使用到了 aggRdd, 导致文件读取两次, 计算两次
     */
    //1 创建sc
    val conf = new SparkConf().setMaster("local[6]").setAppName("cache_prepare")
    val sc = new SparkContext(conf)

// Rdd的处理部分
    //2 读取文件
    val source = sc.textFile("dataset/access_log_sample.txt")

    //3 取出ip，赋予初始频率
    val countRdd = source.map(item => (item.split(" ")(0), 1))

    //4 数据清洗
    val cleanRdd = countRdd.filter(item => StringUtils.isNotEmpty(item._1))

    //5 统计ip出现的次数（聚合）
    var aggRdd = cleanRdd.reduceByKey((curr, agg) => curr + agg)
    aggRdd = aggRdd.cache()

// 两个Rdd的Action操作
    //6 统计出现次数最少的ip（得出结论）
    val lessIp = aggRdd.sortBy(item => item._2, ascending = true).first()

    //7 统计出现次数最多的ip（得出结论）
    val moreIp = aggRdd.sortBy(item => item._2, ascending = false).first()

    println(lessIp, moreIp)
  }

  @Test
  def persist():Unit = {
    //1 创建sc
    val conf = new SparkConf().setMaster("local[6]").setAppName("cache_prepare")
    val sc = new SparkContext(conf)

// Rdd的处理部分
    //2 读取文件
    val source = sc.textFile("dataset/access_log_sample.txt")

    //3 取出ip，赋予初始频率
    val countRdd = source.map(item => (item.split(" ")(0), 1))

    //4 数据清洗
    val cleanRdd = countRdd.filter(item => StringUtils.isNotEmpty(item._1))

    //5 统计ip出现的次数（聚合）
    var aggRdd = cleanRdd.reduceByKey((curr, agg) => curr + agg)
    aggRdd = aggRdd.persist(StorageLevel.MEMORY_ONLY)
    println(aggRdd.getStorageLevel)
// 两个Rdd的Action操作
//    //6 统计出现次数最少的ip（得出结论）
//    val lessIp = aggRdd.sortBy(item => item._2, ascending = true).first()
//
//    //7 统计出现次数最多的ip（得出结论）
//    val moreIp = aggRdd.sortBy(item => item._2, ascending = false).first()
//
//    println(lessIp, moreIp)
  }

  @Test
  def checkpoint():Unit = {
    //1 创建sc
    val conf = new SparkConf().setMaster("local[6]").setAppName("cache_prepare")
    val sc = new SparkContext(conf)
    //设置保存checkpoint的目录，也可以设置为HDFS的目录
    sc.setCheckpointDir("checkpoint")
    // Rdd的处理部分
    //2 读取文件
    val source = sc.textFile("dataset/access_log_sample.txt")

    //3 取出ip，赋予初始频率
    val countRdd = source.map(item => (item.split(" ")(0), 1))

    //4 数据清洗
    val cleanRdd = countRdd.filter(item => StringUtils.isNotEmpty(item._1))

    //5 统计ip出现的次数（聚合）
    var aggRdd = cleanRdd.reduceByKey((curr, agg) => curr + agg)
    // 不准确的说，checkpoint是一个action操作，也就是说
    // 如果调用checkpoint，则会重新计算一下rdd，然后把结果存在HDFS或者本地目录下
    // 所以应该在checkpoint之前，进行一次cache
    aggRdd = aggRdd.cache()
    aggRdd.checkpoint()

    //两个Rdd的Action操作
    //6 统计出现次数最少的ip（得出结论）
    val lessIp = aggRdd.sortBy(item => item._2, ascending = true).first()

    //7 统计出现次数最多的ip（得出结论）
    val moreIp = aggRdd.sortBy(item => item._2, ascending = false).first()

    println(lessIp, moreIp)
  }

}
