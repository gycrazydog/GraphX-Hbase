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
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.client.Scan
import scala.collection.mutable.Buffer
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.collection.JavaConverters._
object GraphReader {
  val tableName = "TestGraph";
  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE,tableName)
  def read(sc : SparkContext):Graph[Any,String] = {
  
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
        println()
      })
      var users : RDD[(VertexId,(Any))]= hBaseRDD.map( v=> (Bytes.toString(v._1.get()).toLong,())) 
      
      println("before flatmap!")
      var relationships = hBaseRDD.flatMap(v=> {
        val result : Result = v._2
        val pp  = result.listCells.asScala.map ( cell => 
          if(Bytes.toString(cell.getQualifier)!="")
            Edge(Bytes.toString(v._1.get()).toLong, Bytes.toString(cell.getValue).toLong,Bytes.toString(cell.getQualifier)) 
          else
            Edge(Bytes.toString(v._1.get()).toLong, Bytes.toString(v._1.get()).toLong,Bytes.toString(cell.getQualifier)) 
        )
        pp  
      })
      val edges = relationships.filter(edge => edge.attr!="")
      val defaultUser = ("John Doe", "Missing")
      // Build the initial Graph
      val graph = Graph(users, edges, defaultUser)
      graph
  }
  def automata(sc : SparkContext) : Graph[Any,String] = {
     val states : RDD[(VertexId,(Any))]= sc.parallelize(Array(
                      (1L, ()), 
                      (2L, ()),
                       (3L, ())
                       ))
      val trans = sc.parallelize(Array(
                    Edge(1L, 2L, "category"),    
                    Edge(2L, 3L, "Entertainment"),
                    Edge(1L,3L,"324")
                  ))
      val defaultUser = ("John Doe", "Missing")
      val graph = Graph(states, trans,defaultUser)
      graph
  }
  def getEdgesByIds(sc: SparkContext,ids : RDD[VertexId]): RDD[Edge[String]] = {
      val hbaseContext = new HBaseContext(sc, conf)
      val inputRdd = ids.map(v=>Bytes.toBytes(v.toString()))
      val getRdd = hbaseContext.bulkGet[Array[Byte], Result](
          "TestGraph",  //The table we want to get from
          2,  //Get list batch size.  Set this somewhere under 1000
          inputRdd,  //RDD that hold records that will turn into Gets
          record => {    //Function that will take a given record to a Get
            new Get(record)
          },
          (result: Result) => {result}
      )
      val resRdd = getRdd.flatMap(v=> {
        val result : Result = v
        val pp  = result.listCells.asScala.map ( cell => 
          if(Bytes.toString(cell.getQualifier)!="")
            Edge(Bytes.toString(result.getRow).toLong, Bytes.toString(cell.getValue).toLong,Bytes.toString(cell.getQualifier)) 
          else
            Edge(Bytes.toString(result.getRow).toLong, Bytes.toString(result.getRow).toLong,Bytes.toString(cell.getQualifier)) 
        )
        pp  
      })
//      val edges = resRdd.filter(edge => edge.attr!="")
//      println("after filter!"+edges.count())
      resRdd
  }
  def getAllVerticesId(sc:SparkContext): RDD[VertexId] = {
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], 
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD.map( v=> Bytes.toString(v._1.get()).toLong)
  }
  def getEdgesByLabels(sc:SparkContext,ColumnFamily:String,Columns : Array[String]) : RDD[Edge[String]] = {
      println("enter getEdges!!")
      val hbaseContext = new HBaseContext(sc, conf)
      val arr = sc.parallelize(Array.range(1,27).map(i=>Bytes.toBytes(i.toString())))
      val getRdd = hbaseContext.bulkGet[Array[Byte], Result](
      "TestGraph",  //The table we want to get from
      100,  //Get list batch size.  Set this somewhere under 1000
      arr,  //RDD that hold records that will turn into Gets
      record => {    //Function that will take a given record to a Get
        var rec = new Get(record)
        Columns.map(v=>rec.addColumn(Bytes.toBytes(ColumnFamily), Bytes.toBytes(v)))
        rec
      },
      (result: Result) => {  //Function that will take a given result and return a serializable object
        result
      }).filter(v=>(!v.isEmpty())).flatMap(v=> {
        val result : Result = v
        val pp  = result.listCells.asScala.map ( cell => 
          if(Bytes.toString(cell.getQualifier)!="")
            Edge(Bytes.toString(result.getRow).toLong, Bytes.toString(cell.getValue).toLong,Bytes.toString(cell.getQualifier)) 
          else
            Edge(Bytes.toString(result.getRow).toLong, Bytes.toString(result.getRow).toLong,Bytes.toString(cell.getQualifier)) 
        )
        pp  
      })
      getRdd.cache()
      getRdd
  }
}