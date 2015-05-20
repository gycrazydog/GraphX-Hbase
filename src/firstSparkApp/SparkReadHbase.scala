package firstSparkApp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.graphx.PartitionStrategy._
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.slf4j.impl.Log4jLoggerFactory
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.collection.immutable.HashSet
import scala.collection.JavaConverters._
object SparkReadHbase {
  def main(args : Array[String]){

    val sparkConf = new SparkConf().setAppName("HBaseMultipleThread").setMaster("local[4]")
      val sc = new SparkContext(sparkConf)
      val automata = GraphReader.automata(sc)
      val finalState = HashSet(3L)
      val startTime = System.currentTimeMillis 
      var ans : Array[VertexId] = Array()
      var currentTrans = automata.edges.filter(e=>e.srcId==1L)
      var currentLabels = currentTrans.map(e=>e.attr).collect()
      val firstEdges = GraphReader.getEdgesByLabels(sc,"Cells",currentLabels)
      var currentStates : RDD[(Edge[String],Edge[String])] = currentTrans.cartesian(firstEdges).filter(f=>f._1.attr.equals(f._2.attr)).repartition(4)
      println("count : " +currentStates.partitions.length)
      var i = 0
      while(currentStates.count()>0){
        i = i+1
        println("iteration:"+i)
        println("partitions : " +currentStates.partitions.length)
//        println("current States:")
//        currentStates.collect().map(println)
        ans = ans ++ currentStates.filter(v=>finalState.contains(v._1.dstId)).map(v=>v._2.dstId).collect()
        println("after concate")
//        var currentNodes = currentStates.map(f=>f._2).map(e=>e.dstId)
        currentTrans = currentTrans.cartesian(automata.edges).filter(v=>v._1.dstId==v._2.srcId).map(v=>v._2).repartition(4)
        currentLabels = currentTrans.map(e=>e.attr).collect()
        currentTrans.cache()
        println("after labels")
        val nextEdges = GraphReader.getEdgesByLabels(sc,"Cells",currentLabels)
        println("after nextedges")
        var nextStates = currentTrans.cartesian(nextEdges).filter(f=>f._1.attr.equals(f._2.attr)).repartition(4)
        println("after nextStates")
        nextStates.cache()
        currentStates = currentStates.cartesian(nextStates).filter(f=>{
          val currentState = f._1
          val nextState = f._2
          (currentState._1.dstId==nextState._1.srcId)&&(currentState._2.dstId==nextState._2.srcId)
        }).map(s=>s._2).repartition(4)
        currentStates.cache()
        println("after currentStates!")
      }
    println("ahahahaha")
      val endTime = System.currentTimeMillis
      ans.map(v=>println("vertex reached!!! "+v))
      println("time : "+(endTime-startTime))
  }
}