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
object CascadeJoinBaseline {
  var path = "";
  var tableName = "testgraph";
  var keyspace = "";
  case class State(startid : Long,srcid : Long, edge : Edge[String])
  def run(sc:SparkContext,workerNum:Int):Set[(Long,Long)] = {
    println("------------------------------start"+path+"--------------------------")
    val auto = GraphReader.automata(sc,path)
    val automata = auto.edges
    val finalState = HashSet(auto.vertices.count().toLong)
    val startTime = System.currentTimeMillis 
    var ans : Set[(VertexId,VertexId)] = new HashSet()
    var currentTrans = automata.filter(e=>e.srcId==1L)
    val labelset = "("+currentTrans.map(v=>"'"+v.attr+"'").collect.mkString(",")+")"
    //RDD[((edge.dstid,autoedge.dstid),edge.startid)]
    var currentStates : RDD[((VertexId,VertexId),VertexId)] = sc.cassandraTable(keyspace, tableName)
                    .where("label IN "+labelset)
                    .flatMap(v=>v.getString("dstid").split(":")
                    .map(d=>(v.getString("label"),(v.getLong("srcid"),d.split("-")(0).toLong) ) ))
                    .join(currentTrans.map(e=>(e.attr,e)))
                    .map(f=>((f._2._1._2,f._2._2.dstId),f._2._1._1))
                    .cache()
      var size = currentStates.count()
      var visitedStates : RDD[((VertexId,VertexId),VertexId)] = sc.emptyRDD
      var i = 0
      while(size>0){
//        currentStates.collect().foreach(println("current state : ",_))
        val nextTrans = currentTrans.map(v=>(v.dstId,v))
                        .join(automata.map(v=>(v.srcId,v)))
                        .map(v=>v._2._2)
                        .distinct
        currentTrans = nextTrans
        val nextTotalStates = visitedStates.union(currentStates).coalesce(workerNum)
        visitedStates = nextTotalStates
//        println("CUrrent Partitions : "+currentStates.partitions.length)
//        println("Current States : "+nextTotalStates.count())
//        println("Current Trans : "+currentTrans.length)
        i = i+1
        println("iteration:"+i)
        println("current States size :"+size)   
        //        currentStates.collect.foreach(v=>println("current State : "+v))
        ans = ans ++ currentStates.filter(v=>finalState.contains(v._1._2)).map(v=>(v._2,v._1._1)).collect()
        val labelset = "("+currentTrans.map(v=>"'"+v.attr+"'").collect().mkString(",")+")"
        println("Answer Size : "+ans.size)
        //RDD[((edge.srcid,autoedge.srcid),(autoedge.dstid,edge.dstid))]
        val nextEdges = sc.cassandraTable(keyspace, tableName)
                    .where("label IN "+labelset)
                    .flatMap(v=>v.getString("dstid").split(":")
                    .map(d=>(v.getString("label"),(v.getLong("srcid"),d.split("-")(0).toLong) ) ))
                    .join(currentTrans.map(e=>(e.attr,e)))
                    .map(f=>((f._2._1._1,f._2._2.srcId),(f._2._1._2,f._2._2.dstId)))
                    .cache()
        val nextStates = nextEdges
                        .join(currentStates)
                        .map(v=>v._2)
                        .subtract(visitedStates) 
                        //.filter(!visitedStates.contains(_))
                        .distinct()
                        .cache()
//        println("iteration : "+i+ " count : "+nextStates.collect())
        currentStates = nextStates
        size = currentStates.count()
        println("finishing calculating currentStates!")
      }
      val endTime = System.currentTimeMillis
//      ans.map(v=>println("vertex reached!!! "+v))
      println("number of pairs : "+ans.size)
      println("time : "+(endTime-startTime))
      println("-------------------------------------------------------------")
      ans
  }
  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName("conjunctive solve and merge : ").setMaster("local[3]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    path = args(0)
    tableName = args(2)
    keyspace = args(1)
    val ans = run(sc,3)
  }
}