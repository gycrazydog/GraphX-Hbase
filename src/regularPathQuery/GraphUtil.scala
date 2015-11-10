package regularPathQuery
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
  var vertexPartition: HashMap[Int,Int] = new HashMap()
  var vertexSet : HashSet[Int] = HashSet()
  var vertexList : List[Int] = List()
  var tableName = "testgraph";
  var keyspace = "";
  var inputnodes : Set[Int] = new HashSet()
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
      val src = partRdd.map(_.getInt("srcid")).collect.toList.distinct
      vertexPartitions.put(idx,src)
//      src.foreach(v=>println("partition # ",idx," node : ",v))
      //.filter(f=>f._2).map(f=>f._1)
    }
    vertexPartitions.foreach(v=>println("partition size : ",v._2.length))
  }
  def getInputNodesNumberAndCrossEdges(sc:SparkContext) = {
    val rdd = sc.cassandraTable(keyspace, tableName)
    println("length : ",rdd.partitions.length)
    val parts = rdd.partitions
    for (p <- parts) {
      val idx = p.index
      val partRdd = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[CassandraRow]) => if (index == idx) it else Iterator(), true)
      val src = partRdd.map(_.getInt("srcid")).collect.toList.removeDuplicates
      src.foreach(vertexPartition.put(_,idx))
    }
    println("vertex partition size "+vertexPartition.size)
//    vertexPartition.foreach(println("vertex map : ",_))
    val rdd1 = rdd.flatMap(row=>row.getString("dstid").split(":")
                    .map(v=>(row.getInt("srcid"),row.getString("label"),v.toInt))
                ).filter(_._3!=(-1))
                .filter(f=>vertexPartition.get(f._1).get!=vertexPartition.get(f._3).get)
                .cache()
    val counter = rdd1.count()         
    val number = rdd1.map(f=>f._3).distinct().count()
    println("input node size "+number)
    println("cross edges size "+counter)
  }
  
  def main(args:Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("GraphWriter").setMaster("local[3]")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    tableName = args(1)
    keyspace = args(0)
    printPartitions(sc)
//    val rdd = sc.cassandraTable(keyspace, tableName).map(v=>v.getString("label")).distinct()
//    println("label size : "+rdd.count())
//    getInputNodesNumberAndCrossEdges(sc)
//    rdd.foreachPartition(x=>println("partition size : ",x.size))
//    writeAlibaba(sc)
//    setOutputNodes(sc)
//    setInputNodes(sc)
//    mapGraph(sc)
//    printMap(sc)
//    printMetisMap(sc)
  }
}