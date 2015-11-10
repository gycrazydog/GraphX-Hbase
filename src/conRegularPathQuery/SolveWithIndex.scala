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
import regularPathQuery.GraphReader
import scala.collection.mutable.BitSet
import java.io._
import scala.io.Source
import scala.collection.mutable.Queue
//import com.madhukaraphatak.sizeof.SizeEstimator;
object SolveWithIndex {
  var path = "";
  var tableName = "testgraph";
  var keyspace = "";
  var maxLevel = 0
  var tree : Array[(Int,List[Vertex])] = Array.empty
  var levels = 0
  case class SrcId(srcid : Long) 
  class Vertex(
    var id : Long,
    var vertexSig : BitSet
  )
  case class State(startid : Long,srcid : Long, edge : Edge[String])
  def loadIndex(){
    val files = new File("/home/crazydog/workspace/Spark test/vs-tree/").list
    levels = files.size-1
    val temp = files.map(s=>(s.split("\\.")(0).toInt,s)).filter(_._1>=maxLevel)
    tree = temp.map(n=>{
      val lines = Source.fromFile("/home/crazydog/workspace/Spark test/vs-tree/"+n._2).getLines()
      val vertices = lines.map(l=>{
          var bitSet = BitSet.empty
          val values = l.split(" ")(1).split(",").map(v=>v.toInt)
          values.foreach(v=>bitSet+=v)
          new Vertex(l.split(" ")(0).toLong,bitSet)
      }).toList
      (n._1,vertices)
    }).sortBy(_._1).reverse
//    println("size of tree is " +SizeEstimator.estimate(tree))
//    tree.foreach(v=>v._2.foreach(k=>println(v._1+" "+k.id+" "+k.vertexSig.size)))
  }
  def lookupIndex(querySig : BitSet) : List[Long] = {
    var ans : List[Long] = List()
    var vQueue = Queue[(Int,Vertex)]((0,tree(0)._2(0)))
    println("ahahaha "+querySig)

    while(vQueue.size>0){
      val head = vQueue.dequeue()
      if((querySig&head._2.vertexSig).equals(querySig)){
        if(head._1+1<tree.size){
          val left = tree(head._1+1)._2(head._2.id.toInt*2)
          vQueue+= ( ((head._1+1),left) )
          if(head._2.id.toInt*2+1<tree(head._1+1)._2.size){
            val right = tree(head._1+1)._2(head._2.id.toInt*2+1)
            vQueue+= ( ((head._1+1),right) )
          }
        }
        else{
//          println(head._2.id*(1<<(levels-head._1))+" "+(head._2.id+1)*(1<<(levels-head._1)))
//          println(querySig&head._2.vertexSig)
          ans ++= List.range(head._2.id*(1<<(levels-head._1)),(head._2.id+1)*(1<<(levels-head._1)) )
        }
      }
    }
    ans
  }
  def run(sc:SparkContext,workerNum:Int):Set[((Long,Long),Long)] = {
    println("------------------------------start"+path+"--------------------------")
    val auto = GraphReader.automata(sc,path)
    val automata = auto.edges
    val finalState = HashSet(auto.vertices.count().toLong)
    val startTime = System.currentTimeMillis 
    var ans : Set[((VertexId,VertexId),VertexId)] = new HashSet()
    var currentTrans = automata.filter(e=>e.srcId==1L)
    val labelset = "("+currentTrans.map(v=>"'"+v.attr+"'").collect.mkString(",")+")"
    val labelcount = currentTrans.count()
    var startSet = BitSet()
    currentTrans.collect.foreach(startSet+=_.attr.toInt)
    val startnodes = lookupIndex(startSet)
    println("startnodes size : "+startnodes.size)
    val startEdges = sc.parallelize(startnodes,workerNum)
                    .map(SrcId(_))
                    .joinWithCassandraTable(keyspace, tableName)
                    .where("label IN "+labelset)
                    .map(v=>(v._2.getInt("srcid"),(v._2.getString("label"),v._2.getString("dstid"))))
                    .groupBy(_._1)
                    .flatMap(v=>v._2)
    startEdges.collect().foreach(println("start state : ",_))
    //RDD[((edge.dstid,autoedge.dstid),(edge.startid,autoedge.lastid))
    var currentStates : RDD[((VertexId,VertexId),(VertexId,VertexId))] = startEdges
                    .flatMap(v=>v._2._2.split(":")
                    .map(d=>(v._2._1,(v._1.toLong,d.split("-")(0).toLong) ) ))
                    .join(currentTrans.map(e=>(e.attr,e)))
                    .map(f=>((f._2._1._2,f._2._2.dstId),(f._2._1._1,f._2._2.srcId)))
                    .cache()
      var size = currentStates.count()
      var visitedStates : RDD[((VertexId,VertexId),(VertexId,VertexId))] = sc.emptyRDD
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
        ans = ans ++ currentStates.filter(v=>finalState.contains(v._1._2)).map(v=>((v._2._1,v._1._1),v._2._2)).collect()
        val labelset = "("+currentTrans.map(v=>"'"+v.attr+"'").collect().mkString(",")+")"
        println("Answer Size : "+ans.size)
        //currentStates : RDD[((edge.dstid,autoedge.dstid),(edge.startid,autoedge.lastid))
         //nextEdges : RDD[((edge.srcid,autoedge.srcid),(edge.dstid,autoedge.dstid))
        val nextEdges = sc.cassandraTable(keyspace, tableName)
                    .where("label IN "+labelset)
                    .flatMap(v=>v.getString("dstid").split(":")
                    .map(d=>(v.getString("label"),(v.getLong("srcid"),d.split("-")(0).toLong) ) ))
                    .join(currentTrans.map(e=>(e.attr,e)))
                    .map(f=>((f._2._1._1,f._2._2.srcId),(f._2._1._2,f._2._2.dstId)))
                    .cache()
        val nextStates = nextEdges
                        .join(currentStates)
                        .map(v=>(v._2._1,(v._2._2._1,v._1._2) ))
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
      val finalAns = ans.groupBy(_._1).map(s=>(s._1,s._2.map(v=>v._2))).filter(s=>s._2.size==automata.filter(a=>finalState.contains(a.dstId)).count)
      finalAns.map(v=>println("pair found : "+v))
      println("number of pairs : "+finalAns.size)
      println("time : "+(endTime-startTime))
      println("-------------------------------------------------------------")
      ans.filter(a=>finalAns.contains(a._1))
  }
  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName("conjunctive solve and merge : ").setMaster("spark://ubuntu:7077")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.eventLog.enabled ","true")
    val sc = new SparkContext(sparkConf)
    path = args(0)
    keyspace = args(1)
    tableName = args(2)
    loadIndex
    println(sc.parallelize(tree.map(f=> (f._1,f._2.map(v=>(v.id,v.vertexSig) ) ) ), 3).count)
//    val ans = run(sc,3)
  }
}