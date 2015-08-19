package firstSparkApp
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
object SparkReadHbase {
  case class SrcId(srcid : Long) 
  case class DstId(dstid : Long) 
  case class Arrow(srcid: Long,label: String)
  case class State(srcid : Long, edge : Edge[String])
  var path = "";
  var tableName = "testgraph";
  var keyspace = "";
  def multipleThreads(workerNum:Int):Unit = {
      val sparkConf = new SparkConf().setAppName("CassandraMultipleThread : "+path).setMaster("local[3]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      val sc = new SparkContext(sparkConf)
      println("------------------------------- start --------------------------")
      val auto = GraphReader.automata(sc,path)
      val automata = auto.edges.collect()
      val finalState = HashSet(auto.vertices.count().toLong)
      val startTime = System.currentTimeMillis 
      var ans : Array[VertexId] = Array()
      var currentTrans = automata.filter(e=>e.srcId==1L)
      var currentStates : RDD[(VertexId,VertexId)] = GraphReader.firstEdges(sc, keyspace, tableName, currentTrans.map(v=>v.attr))
      .flatMap(v=>currentTrans.map(t=>(t,v)))
      .filter(s=>s._2._2==s._1.attr).map(f=>(f._1.srcId,f._2._1))
      .cache()
      var size = currentStates.count()
      var visitedStates : RDD[(VertexId,VertexId)] = sc.emptyRDD
      var i = 0
      while(size>0){
        val nextTotalStates = visitedStates.union(currentStates).coalesce(10)
        visitedStates = nextTotalStates
//        println("CUrrent Partitions : "+currentStates.partitions.length)
//        println("Current States : "+nextTotalStates.count())
//        println("Current Trans : "+currentTrans.length)
        i = i+1
        println("iteration:"+i)
//        println("current States:")
        ans = ans ++ currentStates.filter(v=>finalState.contains(v._1)).map(v=>v._2).collect()
        val labelset = "("+currentTrans.map(v=>"'"+v.attr+"'").mkString(",")+")"
        println("Answer Size : "+ans.size)
        val nextStates = currentStates
                        .flatMap(s=>currentTrans.map { e => (s,e) })
                        .filter(v=>v._1._1==v._2.srcId)
                        .map(v=>State(v._1._2,v._2))
                        .joinWithCassandraTable(keyspace,tableName)
                        .where("label IN "+labelset)
                        .filter(tuple=>tuple._1.edge.attr.equals( tuple._2.get[String]("label") ) )
                        .flatMap(row=>row._2.get[String]("dstid").split(":").map(v=>(row._1.edge.dstId,v.toLong)) )
                        .subtract(visitedStates)
                        //.filter(!visitedStates.contains(_))
                        .distinct()
                        .cache()
//        println("iteration : "+i+ " count : "+nextStates.collect())
        currentStates = nextStates
        size = currentStates.count()
        val nextTrans = currentTrans
                        .flatMap(ct=>automata.map(x=>(ct,x)))
                        .filter(v=>v._1.dstId==v._2.srcId)
                        .map(v=>v._2)
                        .distinct
        currentTrans = nextTrans
        println("finishing calculating currentStates!")
      }
      val endTime = System.currentTimeMillis
      ans.map(v=>println("vertex reached!!! "+v))
      println("number of pairs : "+ans.size)
      println("time : "+(endTime-startTime))
      println("-------------------------------------------------------------")
    }
    def main(args:Array[String]){
      path = args(0)
      tableName = args(2)
      keyspace = args(1)
      multipleThreads(args(3).toInt)
    }
}