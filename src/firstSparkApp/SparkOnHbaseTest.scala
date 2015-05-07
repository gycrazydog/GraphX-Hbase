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
import org.apache.hadoop.hbase.client.Scan
object SparkOnHbaseTest {
  def main(args : Array[String]){
    // Nothing to see here just creating a SparkContext like you normally would
  val sparkConf = new SparkConf().setAppName("first spark app!").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  println("after=enter!!!!")
  val columnFamily = "Cells"
  //This is making a RDD of
  //(RowKey, columnFamily, columnQualifier, value)
  val rdd = sc.parallelize(Array(
        (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes("2") ) ) ),
        (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("324"), Bytes.toBytes("3")))),
        (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("category"), Bytes.toBytes("4") ),(Bytes.toBytes(columnFamily), Bytes.toBytes("relate"), Bytes.toBytes("6") ) ) ),
        (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("music"), Bytes.toBytes("5")))),
        (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes(""), Bytes.toBytes("")))),
        (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("video"), Bytes.toBytes("7") ) ) ),
        (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes(""), Bytes.toBytes("")) ) )
       )
      )
   
  //Create the HBase config like you normally would  then
  //Pass the HBase configs and SparkContext to the HBaseContext
  val conf = HBaseConfiguration.create()
//      conf.addResource(new Path("/usr/lib/hbase/hbase-0.94.27/conf/core-site.xml"))
      conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"))
  val hbaseContext = new HBaseContext(sc, conf)
  //Now give the rdd, table name, and a function that will convert a RDD record to a put, and finally
  // A flag if you want the puts to be batched
//  var scan = new Scan()
//    scan.setCaching(100)
//
//    var getRdd = hbaseContext.hbaseRDD( "SparkTest", scan)
//    println(" --- abc")
//    getRdd.foreach(v => println(Bytes.toString(v._1)))
//    println(" --- def")
//    getRdd.collect.foreach(v => println(Bytes.toString(v._1)))
//    println(" --- qwe")
    println("before put")
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
      "TestGraph",
      //This function is really important because it allows our source RDD to have data of any type
      // Also because puts are not serializable
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
        put
      },
      true);
      println("afterput")
    }
  }