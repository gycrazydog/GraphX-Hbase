package firstSparkApp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
object RDDTest {
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("first spark app!").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val x = sc.parallelize(List(List(List("a"),List("e","f")), List(List("b")), List(List("c"), List("d"))))
    println(x)
    val z = x.collect()
    println(z)
    println(x.flatMap(y => y))
    val y = x.flatMap(y => y).collect()
    println(y.length)
  }
}