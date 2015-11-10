package regularPathQuery
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
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import scala.collection.JavaConverters._
object SparkOnHbaseTest {
  def main(args : Array[String]){
    // Nothing to see here just creating a SparkContext like you normally would
  val sparkConf = new SparkConf().setAppName("first spark app!").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  println("after=enter!!!!")
  val columnFamily = "Cells"
  //This is making a RDD of
  //(RowKey, columnFamily, columnQualifier, value)
  val rdd = sc.parallelize(Array(
        (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes("2") ) ) ),
        (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("324"), Bytes.toBytes("3")))),
        (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("category"), Bytes.toBytes("4") ),(Bytes.toBytes(columnFamily), Bytes.toBytes("relate"), Bytes.toBytes("6") ) ) ),
        (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Music"), Bytes.toBytes("5")))),
        (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes(""), Bytes.toBytes("")))),
        (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("video"), Bytes.toBytes("7") ) ) ),
        (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes("8")) ) ),
        (Bytes.toBytes("8"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("428"), Bytes.toBytes("9")) ) ),
        (Bytes.toBytes("9"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("category"), Bytes.toBytes("10") ),(Bytes.toBytes(columnFamily), Bytes.toBytes("comments"), Bytes.toBytes("12") ),(Bytes.toBytes(columnFamily), Bytes.toBytes("relate"), Bytes.toBytes("14") ) ) ),
        (Bytes.toBytes("10"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Entertainment"), Bytes.toBytes("11")) ) ),
        (Bytes.toBytes("11"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes(""), Bytes.toBytes("")) ) ),
        (Bytes.toBytes("12"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("39"), Bytes.toBytes("13")) ) ),
        (Bytes.toBytes("13"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes(""), Bytes.toBytes("")) ) ),
        (Bytes.toBytes("14"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("video"), Bytes.toBytes("15")) ) ),
        (Bytes.toBytes("15"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes("16")) ) ),
        (Bytes.toBytes("16"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("278"), Bytes.toBytes("17")) ) ),
        (Bytes.toBytes("17"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("relate"), Bytes.toBytes("20")),(Bytes.toBytes(columnFamily), Bytes.toBytes("relate"), Bytes.toBytes("21")),(Bytes.toBytes(columnFamily), Bytes.toBytes("category"), Bytes.toBytes("18")) ) ),
        (Bytes.toBytes("18"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Entertainment"), Bytes.toBytes("19")) ) ),
        (Bytes.toBytes("19"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes(""), Bytes.toBytes("")) ) ),
        (Bytes.toBytes("20"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("video"), Bytes.toBytes("7")) ) ),
        (Bytes.toBytes("21"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("video"), Bytes.toBytes("22")) ) ),
        (Bytes.toBytes("22"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes("23")) ) ),
        (Bytes.toBytes("23"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("573"), Bytes.toBytes("24")) ) ),
        (Bytes.toBytes("24"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("category"), Bytes.toBytes("25")) ) ),
        (Bytes.toBytes("25"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Entertainment"), Bytes.toBytes("26")) ) ),
        (Bytes.toBytes("26"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes(""), Bytes.toBytes("")) ) )
       )
      )
   
  //Create the HBase config like you normally would  then
  //Pass the HBase configs and SparkContext to the HBaseContext
  val conf = HBaseConfiguration.create()
//      conf.addResource(new Path("/usr/lib/hbase/hbase-0.94.27/conf/core-site.xml"))
      conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"))
  val hbaseContext = new HBaseContext(sc, conf)
  val arr = sc.parallelize(Array.range(1,3).map(i=>Bytes.toBytes(i.toString())))
  arr.collect().map(i=>i.map(println))
//    println(" --- abc")
//    getRdd.foreach(v => println(Bytes.toString(v._1)))
//This is the method we are going to focus on
//    val getRdd = hbaseContext.bulkGet[Array[Byte], Result](
//      "TestGraph",  //The table we want to get from
//      100,  //Get list batch size.  Set this somewhere under 1000
//      arr,  //RDD that hold records that will turn into Gets
//      record => {    //Function that will take a given record to a Get
//        var rec = new Get(record)
//        rec.addColumn(Bytes.toBytes("Cells"), Bytes.toBytes("id"))
//        rec
//      },
//      (result: Result) => {  //Function that will take a given result and return a serializable object
//        println("what  "+result)
//        result
//      })
//      val resRdd = getRdd.filter(v=>(!v.isEmpty())).flatMap(v=> {
//        val result : Result = v
//        val pp  = result.listCells.asScala.map ( cell => 
//          if(Bytes.toString(cell.getQualifier)!="")
//            Edge(Bytes.toString(result.getRow).toLong, Bytes.toString(cell.getValue).toLong,Bytes.toString(cell.getQualifier)) 
//          else
//            Edge(Bytes.toString(result.getRow).toLong, Bytes.toString(result.getRow).toLong,Bytes.toString(cell.getQualifier)) 
//        )
//        pp  
//      })
//      resRdd.collect().map(v=>println(v.toString()))
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