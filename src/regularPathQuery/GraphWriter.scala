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
object GraphWriter {
  var vertexPartitions : HashMap[Int,List[Int]] = new HashMap()
  var vertexSet : HashSet[Int] = HashSet()
  var vertexList : List[Int] = List()
  var sparkMaster = ""
  var cassandraMaster = ""
  var path = ""
  var tableName = "testgraph";
  var keyspace = "";
  def writeAlibaba(sc:SparkContext):Unit = {
    val nodes = sc.parallelize(Array.range(0,52050), 3)
    val rdd = sc.textFile(path, 3).map(line=>{
                val edge = line.split(" ")
                ( (edge(0),edge(2)) ,edge(1) )
              })
              .reduceByKey((a, b) => a+':'+b).map(v=>(v._1._1,v._1._2,v._2))
    rdd.saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
    val srcid = rdd.map(f=>f._1.toInt).distinct
    val restnode = nodes.subtract(srcid)
    restnode.map(f=>(f,-1,-1)).saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
    println("restnode number : "+restnode.count())
    println("afterput")
  }
  def writeYoutube(sc:SparkContext):Unit = {
    val nodes = sc.parallelize(Array.range(0,15088), 3)
    val rdd = sc.textFile(path, 3).map(line=>{
                val edge = line.split(" ")
                ( (edge(0),edge(2)) ,edge(1) )
              })
              .reduceByKey((a, b) => a+':'+b).map(v=>(v._1._1,v._1._2,v._2))
    rdd.saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
    val srcid = rdd.map(f=>f._1.toInt).distinct
    val restnode = nodes.subtract(srcid)
    restnode.map(f=>(f,-1,-1)).saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
    println("restnode number : "+restnode.count())
    println("afterput")
  }
  def generateYoutubeDataset(sc:SparkContext):Unit = {
    val writer = new PrintWriter(new File("/home/crazydog/YouTube-dataset/data/youtube.graph.txt"))
    for(i<-1 to 5){
       val rdd = sc.textFile("/home/crazydog/YouTube-dataset/data/"+i+"-edges.csv", 3)
                .map(line=>line.split(","))
                .map(line=>Array(line(0).toInt-1,line(1).toInt-1,i).mkString(" "))
                .collect() 
       rdd.foreach(v=>writer.write(v+"\n"))
    }
    writer.close()
//              .reduceByKey((a, b) => a+':'+b).map(v=>(v._1._1,v._1._2,v._2))
      
//    rdd.saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
    println("afterput")
  }
  def writeTest(sc:SparkContext):Unit = {
    val rdd = sc.textFile("/home/crazydog/ALIBABA/alibaba.graph.txt", 3).map(line=>{
                val edge = line.split(" ")
                ( edge(0),edge(2) ,edge(1) )
              })
    rdd.saveToCassandra("test", "testgraph", SomeColumns("srcid","label","dstid"))
    println("afterput")
  }
  def createMitsGraph(sc:SparkContext){
    val leftright = sc.textFile("/home/crazydog/YouTube-dataset/data/youtube.graph.txt", 3)
                    .map(line=>line.split(" "))
                    .map(line=>(line(0).toInt+1,line(1).toInt+1))
//                    .filter(f=>f._1<=26025&&f._2<=26025)
                    .groupByKey()
                    .map(f=>(f._1,f._2.mkString(" ")))
    val rightleft = sc.textFile("/home/crazydog/YouTube-dataset/data/youtube.graph.txt", 3)
                    .map(line=>line.split(" "))
                    .map(line=>(line(1).toInt+1,line(0).toInt+1))
//                    .filter(f=>f._1<=26025&&f._2<=26025)
                    .groupByKey()
                    .map(f=>(f._1,f._2.mkString(" ")))        
    val lines = leftright.union(rightleft).reduceByKey((x,y)=>x+" "+y).sortByKey(true).collect()
//    lines.foreach(println)
    val writer = new PrintWriter(new File("/home/crazydog/YouTube-dataset/metisgraph/youtube.input.metis.txt"))
    var current = 0
    lines.foreach(v=>{
      current = current+1
      while(current<v._1){
        writer.write("\n")
        current = current+1
      }
      writer.write(v._2+"\n")
    })
//    while(current<52050){
//      writer.write("\n")
//      current = current+1
//    }
    writer.close()
  }
  def mapGraph(sc:SparkContext) = {
    val rdd = sc.cassandraTable(keyspace, tableName).select("srcid")
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
    val seq = rdd.map(v=>v.getInt("srcid")).distinct().collect()
    scala.util.Sorting.quickSort(seq)
    val ordered = seq.filter(v=>vertexSet.contains(v)).zip(vertexList).toMap
    println("ordered size : ",ordered.size)
    //ordered.foreach(println("vertexmap : ",_))
    val lines = sc.textFile(path, 3).map(line=>{
                val edge = line.split(" ")
                val dst = 
                          if(ordered.contains(edge(1).toInt)) ordered.get(edge(1).toInt).get.toString()
                          else edge(1)
                Array(ordered.get(edge(0).toInt).get.toString()
                    ,dst
                    ,edge(2)).mkString(" ")
              }).collect()
    val writer = new PrintWriter(new File("/home/crazydog/YouTube-dataset/ordered/youtube.graph.ordered.txt"))
    lines.foreach(v=>writer.write(v+"\n"))
    writer.close()
  }
  def mapMetisGraph(sc:SparkContext) = {
    val rdd = sc.cassandraTable(keyspace, tableName).select("srcid")
    val parts = rdd.partitions
    for (p <- parts) {
      val idx = p.index
      val partRdd = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[CassandraRow]) => if (index == idx) it else Iterator(), true)
      val src = partRdd.map(_.getInt("srcid")).collect.toList.removeDuplicates
      vertexSet = vertexSet ++ src
      vertexPartitions.put(idx, src)
    }
    vertexList = vertexPartitions.values.toArray.sortBy(_.size).toList.flatten
    println("vertexset : ",vertexSet.size)
    println("vertexlist : ",vertexList.size)
    vertexPartitions.foreach(v=>println("partition size : ",v._2.length))
//    val seq = List.range(0, 26025, 1)
    val ordered = sc.textFile("/home/crazydog/YouTube-dataset/metisgraph/youtube.input.metis.txt.part.3")
                    .map(v=>v.toInt)
                    .zipWithIndex
                    .groupByKey
                    .map(v=>v._2)
                    .sortBy(_.size)
                    .toArray()
                    .flatten
                    .zip(vertexList).toMap
    println("ordered size : ",ordered.size)
    //ordered.foreach(println("vertexmap : ",_))
    val lines = sc.textFile("/home/crazydog/YouTube-dataset/data/youtube.graph.txt", 3).map(line=>{
                val edge = line.split(" ")
                val dst = 
                          if(ordered.contains(edge(1).toInt)) ordered.get(edge(1).toInt).get.toString()
                          else edge(1)
                Array(ordered.get(edge(0).toInt).get.toString()
                    ,dst 
                    ,edge(2)).mkString(" ")
              }).collect()
    val writer = new PrintWriter(new File("/home/crazydog/YouTube-dataset/metisgraph/youtube.graph.metis.txt"))
    lines.foreach(v=>writer.write(v+"\n"))
    writer.close()
  }
  def mapJabeJaGraph(sc:SparkContext) = {
    val rdd = sc.cassandraTable(keyspace, tableName)
    val parts = rdd.partitions
    for (p <- parts) {
      val idx = p.index
      val partRdd = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[CassandraRow]) => if (index == idx) it else Iterator(), true)
      val src = partRdd.map(_.getInt("srcid")).collect.toList.removeDuplicates
      vertexPartitions.put(src.length, src)
    }
    vertexPartitions.foreach(v=>println("partition size : ",v._2.length))
    val ordered = Source.fromFile("/home/crazydog/ALIBABA/jabeja/alibaba.jabeja.inputnode.hybrid.txt").getLines()
                    .map(v=>v.toInt)
                    .zipWithIndex
                    .toArray
                    .groupBy(_._1)
                    .map(v=>(v._2.length,v._2.map(q=>q._2)))
                    .flatMap(v=>vertexPartitions.filter(p=>p._1==v._1).map(p=>v._2.zip(p._2)).flatten)
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
    val writer = new PrintWriter(new File("/home/crazydog/ALIBABA/jabeja/graph.simulated-annealing.txt"))
    lines.foreach(v=>writer.write(v+"\n"))
    writer.close()
  }
  def main(args:Array[String]) : Unit = {
    keyspace = args(0)
    tableName = args(1)
    path  = args(2)
    sparkMaster = args(3)
    cassandraMaster = args(4)
    val sparkConf = new SparkConf().setAppName("GraphWriter").setMaster(sparkMaster)
    .set("spark.cassandra.connection.host", cassandraMaster)
    val sc = new SparkContext(sparkConf)
    writeAlibaba(sc)
//    mapJabeJaGraph(sc)
////    rdd.foreachPartition(x=>println("partition size : ",x.size))
//    writeTest(sc)
//    setOutputNodes(sc)
//    setInputNodes(sc)
//    mapGraph(sc)
//    printMap(sc)
//    mapMetisGraph(sc)
//    createMitsGraph(sc)
//    mapCsvToTxt(sc)
  }
}