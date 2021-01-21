package org.example.taxi

object EitherTest {

  def main(args: Array[String]): Unit = {
    /**
     * 相当于parse方法
     */
    def process(b: Double): Double ={
      val a = 10.0
      a / b
    }

    // Either => left or right
    // Option => Some or None

    def safe(f: Double => Double, b: Double): Either[Double, (Double, Exception)] ={
      try{
        val result = f(b)
        Left(result)
      } catch {
        case e: Exception => Right(b, e)
      }
    }


    val result = safe(process, 0)

    result match {
      case Left(r) => println(r)
      case Right((b,e)) => println(b, e)
    }

  }
}
