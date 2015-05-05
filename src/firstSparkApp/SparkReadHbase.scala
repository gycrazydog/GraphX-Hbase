package firstSparkApp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy._
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
object SparkReadHbase {
  def main(args : Array[String]){

    val tableName = "SparkTest";

    val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample " + tableName ).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

    var scan = new Scan()
    scan.setCaching(100)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], 
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD.foreach(v => {
      print(Bytes.toString(v._1.get()))
      val result = v._2
      val it = result.listCells().iterator()
      while(it.hasNext()){
        val kv = it.next
        print(" "+Bytes.toString(kv.getQualifier)+" "+Bytes.toString(kv.getValue))
      }
    })
    val users: RDD[(VertexId,Unit)] = hBaseRDD.map( v=> (Bytes.toString(v._1.get()).toLong,()) )
    val relationships: RDD[Edge[String]] = hBaseRDD.map(v=> {
      val result = v._2
      val it = result.listCells().iterator()
        val kv = it.next
        Edge(Bytes.toString(v._1.get()).toLong, Bytes.toString(kv.getValue).toLong,Bytes.toString(kv.getQualifier))
    })
    val graph = Graph(users, relationships)
    println(graph.vertices.count())
    println(hBaseRDD.count())
  }
}