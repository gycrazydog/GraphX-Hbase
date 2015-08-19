package firstSparkApp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import com.datastax.spark.connector._
import scala.collection.immutable.HashSet
import scala.collection.mutable.HashMap
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy._
object DanAlgorithm {
  case class SrcId(srcid : Long) 
  case class Complex(srcid: Long,auto: Edge[String],edge: Edge[String])
  var path = "";
  var tableName = "testgraph";
  var keyspace = "";
  def run(workerNum:Int):Unit = {
    val sparkConf = new SparkConf().setAppName("DanAlgorithm : "+path).setMaster("local")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    //val nodes = sc.parallelize( 1 to 26, 3)
    var masterStates : Array[(Edge[String],(Long,Long))] = Array()
    val initialNodes = Array(17L)
    val auto = GraphReader.automata(sc,path)
    val automata = auto.edges.collect()
    var nextAuto: HashMap[VertexId,Array[Edge[String]]] = new HashMap()
    val am = auto.edges.map(v=>(v.srcId,v)).collect()
    am.map(f=>{
        if(nextAuto.contains(f._1))
        nextAuto += (f._1 -> (nextAuto.get(f._1).get:+f._2) )
        else
        nextAuto += (f._1 -> Array(f._2) )  
    })
    val finalState = HashSet(auto.vertices.count().toLong)
    var currentTrans = automata.filter(e=>e.srcId==1L)
    val labelset = "("+currentTrans.map(v=>"'"+v.attr+"'").mkString(",")+")"
    val currentNodes = sc.cassandraTable(keyspace, tableName).where("inputnode = true")
                          .union(sc.cassandraTable(keyspace, tableName).where("label IN "+labelset))
                          .coalesce(3)
    var currentStates = currentNodes.flatMap(x=>automata.map(v=>(v,x)) )
                                      .filter(v=>(v._1.attr==v._2.getString("label")))
                                      .flatMap(f=>f._2.getString("dstid").split(":")
                                      .map(k=>(f._1,(f._2.getInt("srcid").toLong,f._2.getString("label"),k))))
                                      .distinct()
                                      .cache()
    currentStates.collect().foreach(println("init state : ",_))
    var visitedStates = sc.emptyRDD
    var size = currentStates.count()
    while(size>0){
        masterStates = masterStates ++ currentStates.filter(f=>f._2._3.split("-")(1).toInt==0 
                                                            || finalState.contains(f._1.dstId)).collect()
                                                            .map(f=>(f._1,(f._2._1,f._2._3.split("-")(0).toLong)))
                                                            
                                                    
        val nextStates = currentStates.filter(f=>f._2._3.split("-")(1).toInt==1 
                                                            && false==finalState.contains(f._1.dstId) )
                                       .map(f=>Complex(f._2._3.split("-")(0).toLong,f._1,Edge(f._2._1,f._2._3.split("-")(0).toLong,f._2._2)))
                                       .joinWithCassandraTable(keyspace, tableName)
                                       .map(f=>{
                                         val temp = nextAuto.get(f._1.auto.dstId).get
                                                             .filter(x=>x.attr==f._2.getString("label"))
                                         if(temp.length>0)
                                           (Edge(f._1.auto.srcId,temp(0).dstId,temp(0).attr)
                                                ,(f._1.edge.srcId,f._2.getString("label"),f._2.getString("dstid")))
                                         else
                                           (null,(f._1.srcid,f._2.getString("label"),f._2.getString("dstid")))
                                       })
                                       .filter(f=>f._1!=null)
                                       .cache()
        currentStates = nextStates
        size = currentStates.count()
//      val nextGlobalMatches = visitedStates.union(currentStates)
//      visitedStates = nextGlobalMatches
//      val nextStates = currentStates.
    }
    masterStates.foreach(println("state : ",_))
    var ans : Array[(VertexId,VertexId)] = Array()
    while(masterStates.length>0){
      val stopStates = masterStates.filter(p=>p._1.srcId==1L&&finalState.contains(p._1.dstId))
      ans = ans ++  stopStates.map(f=>(f._2._1,f._2._2))
      val nonStopStates = masterStates.filterNot(stopStates.contains)
      var nextAns = nonStopStates.flatMap(f=>nonStopStates.map(v=>(v,f)))
                                  .filter(p=>p._1._1.dstId==p._2._1.srcId&&p._1._2._2==p._2._2._1)
                                  .map(p=>(Edge(p._1._1.srcId,p._2._1.dstId,p._2._1.attr),
                                            (p._1._2._1,p._2._2._2)))
      masterStates = nextAns
    }
    ans.foreach(println("ans : ",_))
  }
  def main(args:Array[String]) = {
    path = args(0)
    tableName = args(2)
    keyspace = args(1)
    run(3)
  }
  
}