package regularPathQuery
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy._
object FirstSpark {
  def main(args:Array[String]){
//    val conf = new SparkConf().setAppName("first spark app!").setMaster("local[3]")
//    val sc = new SparkContext(conf)
//    val users: RDD[(VertexId, (String, String))] = GraphReader.readVertices(sc)
//    
//    val newUsers = users.partitionBy(new RangePartitioner[VertexId, (String,String)](2, users))
//    val newUsers = users.repartition(3)
//     Create an RDD for edges
//    val relationships: RDD[Edge[String]] =
//      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
//                           Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
//      val relationships: RDD[Edge[String]] = GraphReader.readEdges(sc)
//      
//    // Define a default user in case there are relationship with missing user
//    val defaultUser = ("John Doe", "Missing")
//    // Build the initial Graph
//    val graph = Graph(users, relationships, defaultUser).partitionBy(EdgePartition1D, 3)
//    val vertices = graph.vertices
//    val edges = graph.edges
//    println("vertex partition numbers : "+vertices.partitions.length)
//    vertices.foreachPartition(lst=>{
//      println("new vertex partition!" )
//      while(lst.hasNext){
//        val elem = lst.next()
//        println(elem._1+" "+elem._2)
//      }
//    })
//    println("edge partition numbers : "+edges.partitions.length)
//    edges.foreachPartition(lst=>{
//      println("new edge partition!" )
//      while(lst.hasNext){
//        val elem = lst.next()
//        println(elem.srcId+" "+elem.attr+" "+elem.dstId)
//      }
//    })
    
  }
}