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
object GraphWriter {
  var tableName = "testgraph";
  var keyspace = "";
  def writeAlibaba(sc:SparkContext):Unit = {
    val rdd = sc.textFile("/home/crazydog/ALIBABA/alibaba.graph.txt", 3).map(line=>{
                val edge = line.split(" ")
                ( (edge(0),edge(2)) ,edge(1) )
              })
              .reduceByKey((a, b) => a+':'+b).map(v=>(v._1._1,v._1._2,v._2))
    rdd.saveToCassandra(keyspace, tableName, SomeColumns("srcid","label","dstid"))
    println("afterput")
  }
  def writeTest(sc:SparkContext):Unit = {
    val rdd = sc.textFile("/home/crazydog/test/TestGraph.txt", 3).map(line=>{
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
  def main(args:Array[String]) = {
    val sparkConf = new SparkConf().setAppName("GraphWriter").setMaster("local")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    tableName = args(1)
    keyspace = args(0)
    val rdd = sc.cassandraTable(keyspace, tableName).where("inputnode = true")
    println("the input node : ",rdd.count())
//    writeAlibaba(sc)
//    setOutputNodes(sc)
//    setInputNodes(sc)
    //printPartitions(sc)
    
//    println("13000 : "+Bytes.toBytes("13000").map("%02X" format _).mkString)
//    println("14000 : "+Bytes.toBytes("20000").map("%02X" format _).mkString)
//    println("21000 : "+Bytes.toBytes("30000").map("%02X" format _).mkString)
//    val ids = sc.parallelize(Array.range(0,1).map(i=>i.toLong))
//    println("res size:"+GraphReader.getEdgesByIds(sc, ids).count())
  }
}