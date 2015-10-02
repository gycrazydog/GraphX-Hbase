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
object GraphWriter {
  var vertexPartitions : HashMap[Int,List[Int]] = new HashMap()
  var vertexSet : HashSet[Int] = HashSet()
  var vertexList : List[Int] = List()
  var tableName = "testgraph";
  var keyspace = "";
  def writeAlibaba(sc:SparkContext):Unit = {
    val rdd = sc.textFile("/home/crazydog/ALIBABA/metisgraph/graph.txt", 3).map(line=>{
                val edge = line.split(" ")
                ( (edge(0),edge(1)) ,edge(2) )
              })
              .reduceByKey((a, b) => a+':'+b).map(v=>(v._1._1,v._1._2,v._2))
    rdd.saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
    println("afterput")
  }
  def writeTest(sc:SparkContext):Unit = {
    val rdd = sc.textFile("/home/crazydog/test/TestGraph1.txt", 3).map(line=>{
                val edge = line.split(" ")
                ( (edge(0),edge(1)) ,edge(2) )
              })
              .reduceByKey((a, b) => a+':'+b).map(v=>(v._1._1,v._1._2,v._2))
    rdd.saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
    println("afterput")
  }
  def setOutputNodes(sc:SparkContext):Unit = {
    val rdd = sc.cassandraTable(keyspace, tableName)
    println("length : ",rdd.partitions.length)
//    val res = nodes.map(v=>SrcId(v))
//                    //.repartitionByCassandraReplica(keyspace, tableName,3)
//                    //.foreachPartition(v=>v.toList.foreach(println))
//                    .joinWithCassandraTable(keyspace,tableName)
    val parts = rdd.partitions
    for (p <- parts) {
      val idx = p.index
      val partRdd = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[CassandraRow]) => if (index == idx) it else Iterator(), true)
      //The second argument is true to avoid rdd reshuffling
      val data = partRdd.collect //data contains all values from a single partition 
      //println("partition #",idx," ",data.length)
      val src = partRdd.map(_.getInt("srcid")).collect
      //src.foreach(v=>println("partition # ",idx," node : ",v))
      val ans = data.map(f=>{
          val dst = f.get[String]("dstid").split(":")
                     .map(node=>{
                       var outputNode = "-0"
                       if(src.contains(node.toInt))
                       {
                         outputNode = "-1"
                       }
                       val newVal = node.concat(outputNode)
                       newVal
                      } ) 
                     //.filter(node=>src.contains(node.toInt)==false)
         
          (f,dst.mkString(":"))
        }
      ).map(f=>(f._1.getInt("srcid"),f._1.getString("label"),f._2))
      sc.parallelize(ans).saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
      //.filter(f=>f._2).map(f=>f._1)
    }
  }
  def setInputNodes(sc:SparkContext):Unit = {
    var inputNodes: Set[Int] = new HashSet()
    var outputNodes : Set[Int] = new HashSet()
    val rdd = sc.cassandraTable(keyspace, tableName).collect()
              .map (row=>{
                val dst = row.getString("dstid")
                val nodes = dst.split(":")
                nodes.map(node => {
                  val arr = node.split("-")
                  if( arr(1).equals("0"))
                    inputNodes.+= (arr(0).toInt)
                } )
              })
    val res = sc.cassandraTable(keyspace, tableName)
    //.filter(row=>inputNodes.contains(row.getInt("srcid")))
    .map(row=>{
      if(inputNodes.contains(row.getInt("srcid")))
        (row.getInt("srcid"),row.getString("label"),row.getString("dstid"),true)
      else
        (row.getInt("srcid"),row.getString("label"),row.getString("dstid"),false)
    })
    res.saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid","inputnode"))
  }
  def createMitsGraph(sc:SparkContext){
    val leftright = sc.textFile("/home/crazydog/ALIBABA/alibaba.graph.txt", 3)
                    .map(line=>line.split(" "))
                    .map(line=>(line(0).toInt+1,line(1).toInt+1))
//                    .filter(f=>f._1<=26025&&f._2<=26025)
                    .groupByKey()
                    .map(f=>(f._1,f._2.mkString(" ")))
    val rightleft = sc.textFile("/home/crazydog/ALIBABA/alibaba.graph.txt", 3)
                    .map(line=>line.split(" "))
                    .map(line=>(line(1).toInt+1,line(0).toInt+1))
//                    .filter(f=>f._1<=26025&&f._2<=26025)
                    .groupByKey()
                    .map(f=>(f._1,f._2.mkString(" ")))        
    val lines = leftright.union(rightleft).reduceByKey((x,y)=>x+" "+y).sortByKey(true).collect()
//    lines.foreach(println)
    val writer = new PrintWriter(new File("/home/crazydog/ALIBABA/metisgraph/alibaba.metis.txt"))
    var current = 0
    lines.foreach(v=>{
      current = current+1
      while(current<v._1){
        writer.write("\n")
        current = current+1
      }
      writer.write(v._2+"\n")
    })
    while(current<52050){
      writer.write("\n")
      current = current+1
    }
    writer.close()
  }
  def mapGraph(sc:SparkContext) = {
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
    //ordered.foreach(println("vertexmap : ",_))
    val lines = sc.textFile("/home/crazydog/ALIBABA/alibaba.graph.txt", 3).map(line=>{
                val edge = line.split(" ")
                val dst = 
                          if(ordered.contains(edge(1).toInt)) ordered.get(edge(1).toInt).get.toString()
                          else edge(1)
                Array(ordered.get(edge(0).toInt).get.toString()
                    ,edge(2) 
                    ,dst).mkString(" ")
              }).collect()
    val writer = new PrintWriter(new File("/home/crazydog/ALIBABA/alibaba.graph1.txt"))
    lines.foreach(v=>writer.write(v+"\n"))
    writer.close()
  }
  def mapMetisGraph(sc:SparkContext) = {
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
    val ordered = Source.fromFile("/home/crazydog/ALIBABA/alibaba.metis.txt.part.3").getLines()
                    .map(v=>v.toInt)
                    .zipWithIndex
                    .toArray
                    .groupBy(_._1)
                    .map(v=>v._2)
                    .flatten
                    .map(v=>v._2)
                    .zip(vertexList).toMap
    println("ordered size : ",ordered.size)
    //ordered.foreach(println("vertexmap : ",_))
    val lines = sc.textFile("/home/crazydog/ALIBABA/alibaba.graph.txt", 3).map(line=>{
                val edge = line.split(" ")
                val dst = 
                          if(ordered.contains(edge(1).toInt)) ordered.get(edge(1).toInt).get.toString()
                          else edge(1)
                Array(ordered.get(edge(0).toInt).get.toString()
                    ,edge(2) 
                    ,dst).mkString(" ")
              }).collect()
    val writer = new PrintWriter(new File("/home/crazydog/ALIBABA/metisgraph/graph.txt"))
    lines.foreach(v=>writer.write(v+"\n"))
    writer.close()
  }
  def main(args:Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("GraphWriter").setMaster("local")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    tableName = args(1)
    keyspace = args(0)
//    rdd.foreachPartition(x=>println("partition size : ",x.size))
//    writeAlibaba(sc)
//    setOutputNodes(sc)
//    setInputNodes(sc)
//    mapGraph(sc)
//    printMap(sc)
    createMitsGraph(sc)
  }
}