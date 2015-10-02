package firstSparkApp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import com.datastax.spark.connector._
import scala.collection.mutable.HashMap
import scala.collection.immutable.HashSet
import java.io._
import scala.io.Source
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy._
object GraphUtil {
  var vertexPartitions : HashMap[Int,List[Int]] = new HashMap()
  var vertexSet : HashSet[Int] = HashSet()
  var vertexList : List[Int] = List()
  var tableName = "testgraph";
  var keyspace = "";
def printMap(sc:SparkContext) = {
    val rdd = sc.cassandraTable(keyspace, tableName)
    val parts = rdd.partitions
    for (p <- parts) {
      val idx = p.index
      val partRdd = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[CassandraRow]) => if (index == idx) it else Iterator(), true)
      val src = partRdd.map(_.getInt("srcid")).collect.toList.removeDuplicates
      vertexSet = vertexSet ++ src
      vertexList = vertexList ++ src
      vertexPartitions.put(idx, src)
    }
    println("vertexset : ",vertexSet.size)
    println("vertexlist : ",vertexList.size)
    vertexPartitions.foreach(v=>println("partition size : ",v._2.length))
    val seq = List.range(0, 26025, 1)
    val ordered = seq.filter(v=>vertexSet.contains(v)).zip(vertexList).toMap
    println("ordered size : ",ordered.size)
    val writer = new PrintWriter(new File("/home/crazydog/ALIBABA/alibaba.graphmap.txt"))
    ordered.foreach(v=>writer.write(v+"\n"))
    writer.close()
  }
def printMetisMap(sc:SparkContext) = {
    val rdd = sc.cassandraTable(keyspace, tableName)
    val parts = rdd.partitions
    for (p <- parts) {
      val idx = p.index
      val partRdd = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[CassandraRow]) => if (index == idx) it else Iterator(), true)
      val src = partRdd.map(_.getInt("srcid")).collect.toList.removeDuplicates
      vertexSet = vertexSet ++ src
      vertexList = vertexList ++ src
      vertexPartitions.put(idx, src)
    }
    println("vertexset : ",vertexSet.size)
    println("vertexlist : ",vertexList.size)
    vertexPartitions.foreach(v=>println("partition size : ",v._2.length))
    val seq = List.range(0, 26025, 1)
    val ordered = Source.fromFile("/home/crazydog/ALIBABA/metisgraph/alibaba.metis.txt.part.3").getLines()
                    .map(v=>v.toInt)
                    .zipWithIndex
                    .toArray
                    .groupBy(_._1)
                    .map(v=>v._2)
                    .flatten
                    .map(v=>v._2)
                    .zip(vertexList).toMap
    println("ordered size : ",ordered.size)
    val writer = new PrintWriter(new File("/home/crazydog/ALIBABA/metisgraph/alibaba.nodemap.txt"))
    ordered.foreach(v=>writer.write(v+"\n"))
    writer.close()
  }
  def printPartitions(sc:SparkContext) = {
    val rdd = sc.cassandraTable(keyspace, tableName)
    val parts = rdd.partitions
    for (p <- parts) {
      val idx = p.index
      val partRdd = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[CassandraRow]) => if (index == idx) it else Iterator(), true)
      //The second argument is true to avoid rdd reshuffling
      val data = partRdd.collect //data contains all values from a single partition 
      //println("partition #",idx," ",data.length)
      val src = partRdd.map(_.getInt("srcid")).collect
      src.foreach(v=>println("partition # ",idx," node : ",v))
      //.filter(f=>f._2).map(f=>f._1)
    }
  }
  def getCrossEdges(sc:SparkContext) = {
    val rdd = sc.cassandraTable(keyspace, tableName)
                .map(f=>(f.getInt("srcid"),f.getString("label"),f.getString("dstid")))
                .flatMap(p=>p._3.split(":").map(k=>(p._1,p._2,k)))
                .filter(p=>p._3.split("-")(1).toInt==0&&p._3.split("-")(0).toInt<=26024)
    println("crossedges : ",rdd.count())
  }
  def getInputNodesNumber(sc:SparkContext) = {
    val currentNodes = sc.cassandraTable(keyspace, tableName)
                          .where("inputnode = true")
                          .map(v=>v.getInt("srcid"))
                          .distinct()
                          .collect()
    currentNodes.foreach(println("node : ",_))
    println("ans size : ",currentNodes.length)
  }
  
  def main(args:Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("GraphWriter").setMaster("local")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    tableName = args(1)
    keyspace = args(0)
//    getCrossEdges(sc)
//    rdd.foreachPartition(x=>println("partition size : ",x.size))
//    writeAlibaba(sc)
//    setOutputNodes(sc)
//    setInputNodes(sc)
//    mapGraph(sc)
//    printMap(sc)
//    val file = sc.textFile("/home/crazydog/ALIBABA/query/query1_no_nodes.txt", 3)
//                  .filter(v=>v.contains("<<0>>")==false
////                               &&v.contains("<<1>>")==false
//                               )
//                  .collect()
//    val writer = new PrintWriter(new File("/home/crazydog/ALIBABA/query/clean_query.txt"))
//    file.foreach(v=>writer.write(v+"\n"))
//    writer.close()
    
    
//    val rdd = sc.cassandraTable(keyspace, tableName)
//                .flatMap(v=>v.getString("dstid").split(":").map(k=>(v.getString("label"),1)))
//                .reduceByKey((x,y)=>x+y).sortBy(_._2)
//                .collect()
//    rdd.filter(f=>f._2.toDouble/340775>0.0001).foreach(f=>println(f._1,f._2.toDouble/340775))
    
    val file = sc.textFile("/home/crazydog/YouTube-dataset/data/5-edges.csv", 5).cache()
    val count = file.count()
    val rdd =  file.map(v=>v.split(","))
                  .map(v=>(v(2),1))
                  .reduceByKey((x,y)=>x+y).sortBy(_._2)
                  .map(v=>(v._1,v._2.toDouble/count.toDouble))
                  .collect()
    rdd.foreach(println("freq : ",_))  
    
//    printMetisMap(sc)
  }
}