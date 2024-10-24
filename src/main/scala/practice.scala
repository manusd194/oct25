import org.apache.spark.SparkContext

object practice {
  def main(args:Array[String]):Unit=
  {
    val sc=new SparkContext("local[4]","ms")
    println("1. Arrays")
    val arr = Array(20,"manu")
    for (element <- arr) {
      println(element)
    }

    println("2. Strings")
    val str1 = "manu"
    println(str1)

    println("3. Tuples")
    val tuple = (10, "Scala", true)
    val first_element = tuple._1
    println(first_element)
    val second_element = tuple._2
    println(second_element)
    println
    tuple.productIterator.foreach(print)
    tuple.productIterator.mkString(" ").foreach(print)

  }

}



