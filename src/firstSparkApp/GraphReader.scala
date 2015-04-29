package firstSparkApp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy._
object GraphReader {
  def readVertices(sc : SparkContext):RDD[(VertexId, (String, String))] = sc.parallelize(Array(
                      (3L, ("rxin", "student")), 
                      (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), 
                       (2L, ("istoica", "prof")),
                       (4L, ("istoica", "prof"))
                       ))
  def readEdges(sc : SparkContext): RDD[Edge[String]] = sc.parallelize(Array(
                    Edge(3L, 5L, "collab"),    
                    Edge(3L, 2L, "advisor"),
                    Edge(3L, 7L, "colleague")))
}