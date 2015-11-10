package conRegularPathQuery
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import com.datastax.spark.connector._
import scala.collection.immutable.HashSet
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy._
import regularPathQuery.CascadeJoinBaseline
object SolveAndMerge {
  def main(args:Array[String]) = {
    val sparkConf = new SparkConf().setAppName("conjunctive solve and merge : ").setMaster("local[3]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    CascadeJoinBaseline.keyspace="alibaba"
    CascadeJoinBaseline.tableName="graph"
    CascadeJoinBaseline.path = "/home/crazydog/ALIBABA/query/biological/conjunctive/1.txt"
    val temp1 = CascadeJoinBaseline.run(sc,3)
    println("ans1 = "+temp1.size)
    CascadeJoinBaseline.path = "/home/crazydog/ALIBABA/query/biological/conjunctive/2.txt"
    val temp2 = CascadeJoinBaseline.run(sc,3)
    println("ans2 = "+temp2.size)
    val finalAns = temp1.intersect(temp2)
    println("final ans size = "+finalAns.size)
    finalAns.foreach(println("pair found : ",_))
  }
}