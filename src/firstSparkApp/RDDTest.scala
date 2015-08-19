package firstSparkApp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import com.datastax.spark.connector._
object RDDTest {
  def main(args : Array[String]){
    val sparkConf = new SparkConf().setAppName("SparkShuffleTest").setMaster("local")
//                    .set("spark.eventLog.enabled", "true")
      val sc = new SparkContext(sparkConf)
      val ids = sc.parallelize(1000 to 300000,3)
      val now = sc.parallelize(1 to 1200, 3)
      println("ahahaha",ids.union(now).partitions.length)
  }
}