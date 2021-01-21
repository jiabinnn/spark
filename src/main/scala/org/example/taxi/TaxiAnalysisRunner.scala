package org.example.taxi

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.esri.core.geometry.{GeometryEngine, Point, SpatialReference}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

object TaxiAnalysisRunner {
  /**
   * 求的上车时间和下车时间的差值
   * 转为小时
   * 行政区查找，拿到一条在另外一个数据集中查询
   */

  def main(args: Array[String]): Unit = {
    // 1 创建sparksession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("taxi")
      .getOrCreate()

    // 2 导入隐式转换和函数
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 3 数据读取
    val taxiRaw = spark.read
      .option("header", value = true)
      .csv("dataset/half_trip.csv")

    // 4 转换操作
    val taxiParsed: RDD[Either[Trip, (Row, Exception)]] = taxiRaw.rdd.map(safe(parse))
//    可以通过以下方式来过滤出来所有异常的row,
//    val result = taxiParsed.filter(e => e.isRight)
//      .map(e => e.right.get._2) // get._2是异常信息，get._1是异常的值
//    result.foreach(println(_))

    // 如果有异常会报错
    val taxiGood: Dataset[Trip] = taxiParsed.map(either => either.left.get).toDS()

    // 5 绘制时长直方图
    // 编写UDF 完成时长计算，将毫秒转为小时单位
    val hours = (pickUpTime: Long, dropOffTime: Long) => {
      val duration = dropOffTime - pickUpTime
      val hours = TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS)
      hours
    }

    val hoursUDF = udf(hours)
    // 进行统计
//    taxiGood.groupBy(hoursUDF($"pickUpTime", $"dropOffTime") as "duration")
//      .count()
//      .sort("duration")
//      .show()
    /**
     * +--------+-----+
     * |duration|count|
     * +--------+-----+
     * |       0| 9999|
     * |       1|    1|
     * +--------+-----+
     */

    // 6 根据直方图的显示，查看数据分布后，减除反常数据
    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickUpTime, dropOffTime) between 0 and 3")
    taxiClean.show()


    // 7 行政区信息
    // 统计出租车在不同行政区的平均等待时间
//    读取数据集

    val geoJson = Source.fromFile("dataset/nyc-borough-boundaries-polygon.geojson").mkString
    val featureCollection = FeatureExtraction.parseJson(geoJson)
//    排序
    // 后续需要得到每一个出租车在哪个行政区，拿到经纬度，遍历features搜索其所在的行政区
    // 在搜索的过程中，行政区越大，命中的纪律就越高，所以把大的行政区放在前面，更容易命中，减少遍历次数
    val sortedFeatures = featureCollection.features.sortBy(feature => {
      (feature.properties("boroughCode"), -feature.getGeometry().calculateArea2D())
    })
//    广播
    val featureBC = spark.sparkContext.broadcast(sortedFeatures)
//    UDF创建，完成功能
    val boroughLookUp = (x: Double, y: Double)=>{
      // 搜索经纬度所在的行政区
      val featureHit: Option[Feature] = featureBC.value.find(feature => {
        GeometryEngine.contains(feature.getGeometry(), new Point(x, y), SpatialReference.create(4326))
      })
      // 转为行政区信息
      val borough = featureHit.map(feature => feature.properties("borough")).getOrElse("NA")
      borough
    }
//    统计信息
//    val boroughUDF = udf(boroughLookUp)
//    taxiClean.groupBy(boroughUDF($"dropOffX", $"dropOffY"))
//      .count()
//      .show()
    /**
     * +-----------------------+-----+
     * |UDF(dropOffX, dropOffY)|count|
     * +-----------------------+-----+
     * |                 Queens|  869|
     * |                     NA|  306|
     * |               Brooklyn|  751|
     * |          Staten Island|    6|
     * |              Manhattan| 7980|
     * |                  Bronx|   88|
     * +-----------------------+-----+
     */
    // 8 1 过滤没有经纬度的数据
    // 8 2 会话分析
    val sessions = taxiClean.where("dropOffX != 0 and dropOffY != 0 and pickUpX != 0 and pickUpY != 0")
      .repartition($"license")
      .sortWithinPartitions($"license", $"pickUpTime")

    // 8 3 求得时间差
    def boroughDuration(t1: Trip, t2 : Trip):(String, Long) ={
      val borough = boroughLookUp(t1.dropOffX, t1.dropOffY)
      val duration = (t2.pickUpTime - t1.dropOffTime) / 1000
      (borough, duration)
    }

    val boroughDurationSess = sessions.mapPartitions(trips => {
      val viter = trips.sliding(2)
        .filter(_.size == 2)
        .filter(p => p.head.license == p.last.license)
      viter.map(p => boroughDuration(p.head, p.last))
    }).toDF("borough", "seconds")

    boroughDurationSess.where("seconds > 0")
      .groupBy("borough")
      .agg(avg($"seconds"), stddev($"seconds"))
      .show()

    /**
     * +-------------+-----------------+--------------------+
     * |      borough|     avg(seconds)|stddev_samp(seconds)|
     * +-------------+-----------------+--------------------+
     * |       Queens|12886.68787878788|   75886.26477802181|
     * |           NA|28442.14285714286|  115406.49373080398|
     * |     Brooklyn|5758.316205533597|  14728.330072786617|
     * |Staten Island|           8820.0|                 NaN|
     * |    Manhattan|4654.874765352642|  25465.073937246492|
     * |        Bronx|4481.142857142857|    4375.20538773598|
     * +-------------+-----------------+--------------------+
     */
  }



  /**
   * 作用就是封装parse方法，捕获异常
   */
  def safe[P, R](f: P => R): P=> Either[R, (P, Exception)] = {
    new Function[P, Either[R, (P, Exception)]] with Serializable {
      override def apply(param: P): Either[R, (P, Exception)] = {
        try{
          Left(f(param))
        } catch {
          case e: Exception => Right((param, e))
        }
      }
    }

  }

  /**
   * Row -> Trip
   */
  def parse(row: Row): Trip = {
    val richRow = new RichRow(row);
    val license = richRow.getAs[String]("hack_license").orNull
    val pickUpTime = parseTime(richRow, "pickup_datetime")
    val dropOffTime = parseTime(richRow, "dropoff_datetime")
    val pickUpX = parseLocation(richRow, "pickup_longitude")
    val pickUpY = parseLocation(richRow, "pickup_latitude")
    val dropOffX = parseLocation(richRow, "dropoff_longitude")
    val dropOffY = parseLocation(richRow, "dropoff_latitude")

    Trip(
      license = license,
      pickUpTime = pickUpTime,
      dropOffTime = dropOffTime,
      pickUpX = pickUpX,
      pickUpY = pickUpY,
      dropOffX = dropOffX,
      dropOffY = dropOffY
    )
  }

  def parseTime(row: RichRow, field: String): Long = {
    // 表示出来时间类型的格式SimpleDateFormat
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = new SimpleDateFormat(pattern, Locale.ENGLISH)
    // 执行转换，获取Date对象，getTime 获取时间戳
    val time: Option[String] = row.getAs[String](field)
    val timeOption: Option[Long] = time.map(time => formatter.parse(time).getTime)
    timeOption.getOrElse(0L)
  }

  def parseLocation(row: RichRow, field: String): Double = {
    // 获取数据
    val location = row.getAs[String](field)
    // 转换数据
    val locationOption = location.map(loc => loc.toDouble)
    locationOption.getOrElse(0.0D)
  }
}

class RichRow(row: Row) {

  def getAs[T](field: String): Option[T] = {
    // 判断row.getAs是否为空
    if (row.isNullAt(row.fieldIndex(field))) {
      // null -> 返回None
      None
    }
    else {
      // not null -> 返回some
      Some(row.getAs[T](field))
    }
  }
}

/**
 * 代表一个行程, 是集合中的一条记录
 * @param license 出租车执照号
 * @param pickUpTime 上车时间
 * @param dropOffTime 下车时间
 * @param pickUpX 上车地点的经度
 * @param pickUpY 上车地点的纬度
 * @param dropOffX 下车地点的经度
 * @param dropOffY 下车地点的纬度
 */
case class Trip(
                 license: String,
                 pickUpTime: Long,
                 dropOffTime: Long,
                 pickUpX: Double,
                 pickUpY: Double,
                 dropOffX: Double,
                 dropOffY: Double
               )