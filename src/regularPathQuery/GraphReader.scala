package regularPathQuery
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import com.datastax.spark.connector._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import scala.collection.immutable.HashSet
import org.apache.spark.graphx.PartitionStrategy._
object GraphReader {
    def automata(sc : SparkContext,path : String) : Graph[Any,String] = {
     val rdd = sc.textFile(path, 3).cache()
     val max_id = rdd.filter(line=>line.split(" ").size<=2).map(line=>line.split(" ")).toArray()(0)(0).toInt
     val states : RDD[(VertexId,(Any))]= sc.parallelize(1L to max_id,1).cartesian(sc.parallelize(Array(()), 1))
      val trans = rdd.filter(line=>line.split(" ").size==3).map(line=>{
        val edge = line.split(" ")
        Edge(edge(0).toLong,edge(1).toLong,edge(2))
      })
      val defaultUser = ("John Doe", "Missing")
      val graph = Graph(states, trans,defaultUser)
      graph
  }
    def getStartNode(sc : SparkContext,path : String) : Long = {
      val rdd = sc.textFile(path, 3).cache()
      val startnode = rdd.filter(line=>line.split(" ").size==2)
                          .map(line=>line.split(" ")).toArray()(0)(1).toInt
      startnode
    }
    def firstEdges(sc:SparkContext,keyspace: String,tableName: String,Columns : Array[String]) : RDD[(VertexId,String,VertexId)] = {
      println("enter getEdges!!")
      val labelset = "("+Columns.map(v=>"'"+v+"'").mkString(",")+")"
//      val labelset = "('video','category')"
      val rdd = sc.cassandraTable(keyspace, tableName)
      .where("label IN "+labelset)
      .flatMap(v=>v.getString("dstid").split(":")
                    .map(d=>(v.getLong("srcid"),v.getString("label"),d.split("-")(0).toLong) ) )
      rdd.cache()
      rdd
    }
    def getGraphSize(sc: SparkContext,keyspace : String,tableName:String): Long = {
      sc.cassandraTable(keyspace, tableName).count()
    }
    case class SrcId(srcid : Long)
    def getNextStates(sc: SparkContext,keyspace : String,tableName : String,ids : RDD[(VertexId,String)],
        Columns : RDD[Edge[String]],Visited : HashSet[(VertexId, VertexId)]): RDD[(VertexId,VertexId)] = {
//      val labelset = "("+Columns.toArray().map(v=>"'"+v.attr+"'").mkString(",")+")"
      val srcidSet = ids.map(v=>arrow(v._1,v._2)).repartitionByCassandraReplica(keyspace,tableName)
                        .joinWithCassandraTable(keyspace,tableName)
                        .map(v=>v._2).cartesian(Columns).filter(tuple=>{
                          val Automata = tuple._2
                          val edge = tuple._1
                          Automata.attr.equals(edge.getString("label"))&&Visited.contains((Automata.srcId,edge.getLong("srcid")))
                        }).flatMap(row=>row._1.get[Set[Long]]("dstid").map(v=>(row._2.dstId,v)))
//                        .flatMap(row=>row._1.get[Set[Long]]("dstid").map(v=>(row._2.dstId,v)))
//      srcidSet.cache()
//      println("join size : "+srcidSet.collect().foreach(println))
      srcidSet
    }
    case class arrow(srcid: Long,label: String)
    def main(args : Array[String]){
    val sparkConf = new SparkConf().setAppName("CassandraMultipleThread : ").setMaster("local")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      val sc = new SparkContext(sparkConf)
      val ids = sc.parallelize(Array(1L)).map ( x => SrcId(x) )
      val res = ids.joinWithCassandraTable("mykeyspace","example").where("label IN ('kills','loves')");
      res.collect.foreach(println)
  }
}