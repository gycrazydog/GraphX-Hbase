package conRegularPathQuery
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import com.datastax.spark.connector._
import org.apache.spark.graphx.PartitionStrategy._
import scala.collection.mutable.BitSet
import java.io._
class edgeSig(
    var edge : BitSet
)
class Vertex(
  var id : Long,
  var vertexSig : BitSet
)
object VS_tree_Creater {
   case class SrcId(srcid : Long) 
   val keyspace = "alibaba"
   val table = "graph"
   val path = "/home/crazydog/ALIBABA/alibaba.graph.txt"
   def run(){
     val sparkConf = new SparkConf().setAppName("VS_Tree : ").setMaster("local[3]")
     val sc = new SparkContext(sparkConf)
     var rdd = sc.textFile(path, 3).map(line=>{
                  val edge = line.split(" ")
                  Edge(edge(0).toLong,edge(1).toLong,edge(2))
    }).collect()
     val nodes = (rdd.map(f=>f.srcId)++rdd.map(f=>f.dstId)).distinct
     val labels = rdd.map(f=>f.attr.toInt).distinct
     println("the number of label is : "+labels.size)
     var level = 0
     println("nodes level : "+Math.log(nodes.size)/Math.log(2))
     while(level<=(Math.log(nodes.size)/Math.log(2)) ){
       println("level : "+level)
       val G = rdd.groupBy(_.srcId).map(f=>(f._1,f._2.map(e=>BitSet(e.attr.toInt))))
                                   .map(f=>(f._1,f._2.reduceLeft(_|_)))
                                   .map(f=>new Vertex(f._1,f._2))
                                   .toList
                                   .sortBy(_.id)
       val writer = new PrintWriter(new File("/home/crazydog/workspace/Spark test/vs-tree/"+level+".txt"))
       G.foreach(v=>writer.write(v.id+" "+v.vertexSig.mkString(",")+"\n"))
       writer.close()
       val newrdd = rdd.map(f=>Edge(f.srcId/2,f.dstId/2,f.attr))
       rdd = newrdd
       level = level + 1
     }
   }
   def main(args: Array[String]) = {
     run()
//     val sparkConf = new SparkConf().setAppName("VS_Tree : ")
//                                     .setMaster("local[3]")
//                                     .set("spark.cassandra.connection.host", "127.0.0.1")
//     val sc = new SparkContext(sparkConf)
//     val ids = sc.parallelize(List.range(0 , 52050), 3)
//     val startTime = System.currentTimeMillis 
//     val rdd = ids.map(SrcId(_)).joinWithCassandraTable(keyspace, table).where("label in ('0','1')")
//     println("ans : "+rdd.count)
//     val endTime = System.currentTimeMillis
//      println("time : "+(endTime-startTime))
//     var rdd = sc.textFile("/home/crazydog/workspace/Spark test/vs-tree/0.txt", 3).map(line=>{
//                  val edge = line.split(" ")
//                  Vertex(edge(0).toLong,BitSet(edge(1)))
//    }).collect()
   }
}