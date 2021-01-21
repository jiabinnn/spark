package org.example.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * mapPartitions 和 map 算子是一样的，只不过map是针对每一条数据进行转换，mapPartitions针对一整个分区的数据进行转换
 * 所以：
 * 1. map的func参数是单条数据，mapPartitions的func参数是一个集合（一个分区整个所有的数据）
 * 2. map的func返回值也是单条数据，mapPartitions的func返回值是一个集合
 */
class TransformerOp {

  val conf = new SparkConf().setMaster("local[6]").setAppName("transformation_op")
  val sc = new SparkContext(conf)
  @Test
  def mapPartitions():Unit = {
    //1 数据生成
    //2 数据处理
    //3 获取结果
    sc.parallelize(Seq(1,2,3,4,5,6), 2)
      .mapPartitions(iter => {
        iter.foreach(item => println(item))
        iter
      })
      .collect()
  }

  @Test
  def mapPartitions2():Unit = {
    sc.parallelize(Seq(1,2,3,4,5,6), 2)
      .mapPartitions(iter => {
        // 遍历iter 其中每一条数据进行转换，转换完成以后返回这个iter
        // iter 是scala中的集合类型
        iter.map(item => item * 10)

      })
      .collect()
      .foreach(item => println(item))
  }

  /**
   * mapPartitions和mapPartitionsWithIndex的区别是func中多了一个参数，分区号
   */
  @Test
  def mapPartitionsWithIndex():Unit = {
    sc.parallelize(Seq(1,2,3,4,5,6), 2)
      .mapPartitionsWithIndex((index, iter) => {
        println("index: " + index)
        iter.foreach(item => println(item))
        iter
      })
      .collect()
  }

  /**
   * filter可以过滤数据集中的一部分元素
   * 返回true，则保留，返回false，则删除
   */
  @Test
  def filter():Unit = {
    sc.parallelize(Seq(1,2,3,4,5,6), 2)
      .filter(value => value % 2 == 0)
      .collect()
      .foreach(item => println(item))

  }

  /**
   * 1 把大数据集变成小数据集
   * 2 尽量减少对数据集特征对损失
   */
  @Test
  def sample():Unit = {
    val rdd1 = sc.parallelize(Seq(1,2,3,4,5,6,7,8,9,10))
    /**
     * withReplacement:是否有放回的采样
     * fraction: 采样比例
     */
    val rdd2 = rdd1.sample(true, 0.6)
    val result = rdd2.collect()
    result.foreach(item => println(item))
  }

  @Test
  def mapValue():Unit = {
    sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)))
      .mapValues(item => item * 10)
      .collect()
      .foreach(println(_))

  }

  @Test
  def intersection():Unit = {
    val rdd1 = sc.parallelize(Seq(1,2,3,4,5))
    val rdd2 = sc.parallelize(Seq(3,4,5,6,7))
      rdd1.intersection(rdd2)
        .collect()
        .foreach(println(_))
  }

  @Test
  def union():Unit = {
    val rdd1 = sc.parallelize(Seq(1,2,3,4,5))
    val rdd2 = sc.parallelize(Seq(3,4,5,6,7))
    rdd1.union(rdd2)
      .collect()
      .foreach(println(_))
  }

  @Test
  def subtract():Unit = {
    val rdd1 = sc.parallelize(Seq(1,2,3,4,5))
    val rdd2 = sc.parallelize(Seq(3,4,5,6,7))
    rdd1.subtract(rdd2)
      .collect()
      .foreach(println(_))
  }

  @Test
  def groupByKey():Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 1), ("a", 1), ("c", 3), ("c", 4)))
    val rdd1: RDD[(String, Iterable[Int])] =  rdd.groupByKey()
    val rdd2: Array[(String, Iterable[Int])] = rdd1.collect()
    rdd2.foreach(println(_))
  }

  /**
   * combineByKey接受3个参数
   * 转换数据的函数（初始函数，作用于第一条数据，用于开启整个计算），
   * 在分区上进行聚合，把所有分区的聚合结果聚合为最终结果
   */
  @Test
  def combineByKey():Unit = {
    val rdd: RDD[(String, Double)] = sc.parallelize(Seq(
      ("zhangsan", 99.0),
      ("zhangsan", 96.0),
      ("lisi", 97.0),
      ("lisi", 98.0),
      ("zhangsan", 97.0),
    ))
    // createCombiner
    // mergeValue
    // mergeCombiners
    val combineResult = rdd.combineByKey(
      createCombiner = (curr: Double) => (curr, 1),
      mergeValue = (curr: (Double, Int), nextValue: Double) => (curr._1 + nextValue, curr._2 + 1),
      mergeCombiners = (curr: (Double, Int), agg: (Double, Int)) => (curr._1 + agg._1, curr._2 + agg._2)
    )

    val resultRDD = combineResult.map(item => (item._1, item._2._1 / item._2._2))
    resultRDD.collect().foreach(println(_))
  }

  /**
   * foldByKey 和 Spark中的reduceByKey的区别是可以指定初始值
   * foldByKey 和 Scala中的foldLeft，foldRight区别是，这个初始值作用于每一个数据
   */
  @Test
  def foldByKey():Unit = {
    sc.parallelize(Seq(("a", 5), ("a", 1), ("b", 1)))
      .foldByKey(zeroValue = 10)( (curr, agg) => curr + agg )
      .collect()
      .foreach(println(_))
  }

  /**
   * aggregateByKey(zeroValue)(seqOp, combOb)
   * seroValue: 指定初始值
   * seqOp: 作用于每一个元素，根据初始值，进行计算
   * combOp: 将seqOp处理过的结果进行聚合
   *
   * aggregrateByKey 特别适合针对每一个数据要先处理，后聚合
   */
  @Test
  def aggregateByKey():Unit = {
    val rdd = sc.parallelize(Seq(("手机", 10.0), ("手机", 15.0), ("电脑", 20.0)))
    rdd.aggregateByKey(0.8)((zeroValue, item) => item * zeroValue, (curr, agg) => curr + agg)
      .collect().foreach(println(_))
  }

  @Test
  def join():Unit = {
    val rdd1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 1)))
    val rdd2 = sc.parallelize(Seq(("a", 10), ("a", 11), ("d", 12)))

//    rdd1.join(rdd2).collect().foreach(println(_))

    rdd1.leftOuterJoin(rdd2).collect().foreach(println(_))
//    val rdd3: Array[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2).collect()
//    println(rdd3(0)._2._1)
//    println(rdd3(0)._2._2.get)
  }

  @Test
  def sort():Unit = {
    val rdd1 = sc.parallelize(Seq(("a", 3), ("b", 2), ("c", 1)))
    rdd1.sortBy((item => item._2)).collect().foreach(println(_))
//    rdd1.sortByKey(false).collect().foreach(println(_))
  }

  /**
   * repartition 进行重分区的时候，默认是shuffle的
   * coalesce 进行重分区的时候，默认是不shuffle的，coalesce默认不能增大分区数
   */
  @Test
  def repartition():Unit = {
    val rdd = sc.parallelize(Seq(1,2,3,4,5), 2)
//    println(rdd.repartition(2).partitions.size)
    println(rdd.coalesce(5, shuffle = false).partitions.size)
  }
}
