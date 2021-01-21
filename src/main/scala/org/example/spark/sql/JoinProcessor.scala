package org.example.spark.sql

import org.apache.spark.sql.SparkSession
import org.junit.Test

class JoinProcessor {

  private val  spark = SparkSession.builder()
    .master("local[6]")
    .appName("join processor")
    .getOrCreate()

  import spark.implicits._

  private val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3))
    .toDF("id", "name", "cityId")
  person.createOrReplaceTempView("person")

  private val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
    .toDF("id", "name")
  cities.createOrReplaceTempView("cities")

  @Test
  def introJoin(): Unit ={
    val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
      .toDF("id", "name", "cityId")

    val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
      .toDF("id", "name")

    val df = person.join(cities, person.col("cityId") === cities.col("id"))
      .select(person.col("id"),
        person.col("name"),
        cities.col("name") as "city")

      df.show()

    /**
     * +---+------+---------+
     * | id|  name|     name|
     * +---+------+---------+
     * |  0|  Lucy|  Beijing|
     * |  1|  Lily|  Beijing|
     * |  2|   Tim|Guangzhou|
     * |  3|Danial|  Beijing|
     * +---+------+---------+
     */

    df.createOrReplaceTempView("user_city")

    spark.sql("select id, name, city from user_city where city = 'Beijing'")
      .show()

    /**
     * +---+------+-------+
     * | id|  name|   city|
     * +---+------+-------+
     * |  0|  Lucy|Beijing|
     * |  1|  Lily|Beijing|
     * |  3|Danial|Beijing|
     * +---+------+-------+
     */
  }

  @Test
  def crossJoin(): Unit ={
    person.crossJoin(cities)
      .where(person.col("cityId") === cities.col("id"))
      .show()

    spark.sql("select u.id, u.name, c.name from person u cross join cities c where u.cityId = c.id")
      .show()
  }

  @Test
  def inner(): Unit ={
    person.join(cities, person.col("cityId") === cities.col("id"), joinType = "inner")
      .show()

    /**
     * +---+----+------+---+---------+
     * | id|name|cityId| id|     name|
     * +---+----+------+---+---------+
     * |  0|Lucy|     0|  0|  Beijing|
     * |  1|Lily|     0|  0|  Beijing|
     * |  2| Tim|     2|  2|Guangzhou|
     * +---+----+------+---+---------+
     */
    spark.sql("select p.id, p.name, c.name "+
    "from person p inner join cities c on p.cityId = c.id")
      .show()

    /**
     * +---+----+---------+
     * | id|name|     name|
     * +---+----+---------+
     * |  0|Lucy|  Beijing|
     * |  1|Lily|  Beijing|
     * |  2| Tim|Guangzhou|
     * +---+----+---------+
     */
  }

  @Test
  def full_outer(): Unit ={
    person.join(
      cities,
      person.col("cityId") === cities.col("id"),
      joinType = "full")
      .show()

    /**
     * +----+------+------+----+---------+
     * |  id|  name|cityId|  id|     name|
     * +----+------+------+----+---------+
     * |null|  null|  null|   1| Shanghai|
     * |   3|Danial|     3|null|     null|
     * |   2|   Tim|     2|   2|Guangzhou|
     * |   0|  Lucy|     0|   0|  Beijing|
     * |   1|  Lily|     0|   0|  Beijing|
     * +----+------+------+----+---------+
     */

    spark.sql("select p.id, p.name, c.name "+
      "from person p full outer join cities c on p.cityId = c.id")
      .show()

    /**
     * +----+------+---------+
     * |  id|  name|     name|
     * +----+------+---------+
     * |null|  null| Shanghai|
     * |   3|Danial|     null|
     * |   2|   Tim|Guangzhou|
     * |   0|  Lucy|  Beijing|
     * |   1|  Lily|  Beijing|
     * +----+------+---------+
     */
  }

  @Test
  def left_outer(): Unit ={
    // 左外连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "left")
      .show()

    /**
     * +---+------+------+----+---------+
     * | id|  name|cityId|  id|     name|
     * +---+------+------+----+---------+
     * |  0|  Lucy|     0|   0|  Beijing|
     * |  1|  Lily|     0|   0|  Beijing|
     * |  2|   Tim|     2|   2|Guangzhou|
     * |  3|Danial|     3|null|     null|
     * +---+------+------+----+---------+
     */
    spark.sql("select p.id, p.name, c.name "+
      "from person p left join cities c on p.cityId = c.id")
      .show()

    // 右外连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "right")
      .show()

    /**
     * +----+----+------+---+---------+
     * |  id|name|cityId| id|     name|
     * +----+----+------+---+---------+
     * |   1|Lily|     0|  0|  Beijing|
     * |   0|Lucy|     0|  0|  Beijing|
     * |null|null|  null|  1| Shanghai|
     * |   2| Tim|     2|  2|Guangzhou|
     * +----+----+------+---+---------+
     */
    spark.sql("select p.id, p.name, c.name "+
      "from person p right join cities c on p.cityId = c.id")
      .show()
  }

  @Test
  def leftanti_leftsemi(): Unit ={
    /**
     * left_anti
     * 1       =    1
     * 2    A  =
     * 3    B  =
     *      C  =
     */
    // left_anti
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "leftanti")
      .show()

    /**
     * +---+------+------+
     * | id|  name|cityId|
     * +---+------+------+
     * |  3|Danial|     3|
     * +---+------+------+
     */
    spark.sql("select p.id, p.name "+
      "from person p left anti join cities c on p.cityId = c.id")
      .show()

    /**
     * left_semi
     * 1       =
     * 2    A  =    2
     * 3    B  =    3
     *      C  =
     */

    // left_semi
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "leftsemi")
      .show()

    /**
     * +---+----+------+
     * | id|name|cityId|
     * +---+----+------+
     * |  0|Lucy|     0|
     * |  1|Lily|     0|
     * |  2| Tim|     2|
     * +---+----+------+
     */
    spark.sql("select p.id, p.name "+
      "from person p left semi join cities c on p.cityId = c.id")
      .show()

  }
}
