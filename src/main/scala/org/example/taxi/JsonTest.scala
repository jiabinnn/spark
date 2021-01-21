package org.example.taxi

import org.json4s.native.Serialization

object JsonTest {

  def main(args: Array[String]): Unit = {

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    val product =
      """
        |{"name":"Toy","price":35.35}
  """.stripMargin

    // 隐式转换的形式提供格式工具，例如，如何解析时间字符串
    implicit val format = Serialization.formats(NoTypeHints)

    // 具体的解析为某一个对象
    val productObj1 = parse(product).extract[Product]
    println(productObj1)

    // 可以通过一个方法直接将json字符串转为对象，但是这种方式就无法进行搜索了
    import org.json4s.jackson.Serialization.{read, write}
    val productObj2 = read[Product](product)
    println(productObj2)

    // 将对象转为json字符串
    val productObj3 = Product("电视", 10.5)
//    val jsonStr1 = compact(render((productObj3)))
    val jsonStr = write(productObj3)
  }
}

case class Product(name: String, price: Double)
