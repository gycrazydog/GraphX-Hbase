package regularPathQuery
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
object SingleThread {
  case class SrcId(srcid : Long) 
  case class DstId(dstid : Long) 
  case class arrow(srcid: Long,label: String)
  var path = "";
  var tableName = "testgraph";
  var keyspace = "";
  def singleThread():Unit = {
      val sparkConf = new SparkConf().setAppName("CassandraSingleThread : "+path).setMaster("local")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      val sc = new SparkContext(sparkConf)
      println("start !!!")
      val auto = GraphReader.automata(sc,path)
      val automata = auto.edges.repartition(1)
      val finalState = HashSet(auto.vertices.count().toLong)
      val startTime = System.currentTimeMillis 
      var ans : Array[VertexId] = Array()
      var currentTrans = automata.filter(e=>e.srcId==1L)
      var currentStates : RDD[(VertexId,VertexId)] = currentTrans
      .cartesian(GraphReader.firstEdges(sc, keyspace, tableName, currentTrans.map(v=>v.attr).collect()))
      .filter(s=>s._2._2==s._1.attr).map(f=>(f._1.srcId,f._2._1))
      .repartition(1)
      .cache()
      var visitedStates : HashSet[(VertexId,VertexId)] = HashSet()
      var temp = currentStates.collect()
      var i = 0
      while(temp.size>0){
        visitedStates ++= temp
        println("CUrrent Partitions : "+currentStates.partitions.length)
        println("Current States : "+temp.size)
        println("Current Trans : "+currentTrans.count())
        i = i+1
        println("iteration:"+i)
//        println("current States:")
        ans = ans ++ temp.filter(v=>finalState.contains(v._1)).map(v=>v._2)
        val Trans = currentTrans.collect()
        val labelset = "("+Trans.map(v=>"'"+v.attr+"'").mkString(",")+")"
        println("Answer Size : "+ans.size)
        val nextStates = currentStates
                        .map(v=>SrcId(v._2))
                        .joinWithCassandraTable(keyspace,tableName)
                        .where("label IN "+labelset)
                        .map(v=>v._2).flatMap(s=>Trans.map { e => (s,e) })
                        .filter(tuple=>{
                          val Automata = tuple._2
                          val edge = tuple._1
                          Automata.attr.equals(edge.getString("label"))&&visitedStates.contains((Automata.srcId,edge.getLong("srcid")))
                        }).flatMap(row=>row._1.get[String]("dstid").split(":").map(v=>(row._2.dstId,v.toLong)) ).filter(!visitedStates.contains(_))
                        .distinct()
                        .cache()
//        println("iteration : "+i+ " count : "+nextStates.collect())
        val newtemp = nextStates.collect()
        temp = newtemp
        currentStates = nextStates
        val nextTrans = currentTrans.cartesian(automata).filter(v=>v._1.dstId==v._2.srcId).map(v=>v._2).distinct().cache()
        currentTrans = nextTrans
        println("finishing calculating currentStates!")
      }
      val endTime = System.currentTimeMillis
      ans.map(v=>println("vertex reached!!! "+v))
      println("number of pairs : "+ans.size)
      println("time : "+(endTime-startTime))
    }
  case class key(key: Int)
    def main(args:Array[String]){
//      var visitedStates : HashSet[(VertexId,VertexId)] = HashSet()
//      visitedStates+=((1L,3L))
//      println(visitedStates.contains((2L,3L)))
      path = args(0)
      tableName = args(2)
      keyspace = args(1)
      singleThread()
    }
}