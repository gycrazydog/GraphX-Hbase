package regularPathQuery

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy._
import java.io._
import scala.collection.mutable.HashMap
object JabeJa {
  var TEMPERATURE = 2.0
  val TEMPERATUREDelta = 0.01
  var sparkMaster = ""
  var cassandraMaster = ""
  var path = ""
  var tableName = "testgraph";
  var keyspace = "";
  
  var color: Array[Int] = Array()
  var toNeighbors : Map[Int,Set[Int]] = Map.empty
  var fromNeighbors : Map[Int,Set[Int]] = Map.empty
  
  def swap(cur : Int,neigh : Int) :Unit = {
      val temp = color(neigh)
      color(neigh) = color(cur)
      color(cur) = temp;
      
  }
  def JabeJa_Local(sc:SparkContext) = {
  val edges = sc.textFile(path, 3).map(line=>{
                val edge = line.split(" ")
                Edge(edge(0).toLong,edge(1).toLong,edge(2))
  }).collect()
  val writer = new PrintWriter(new File("jabeja-local.round.txt"))
  val neighbors : Map[Int,Array[(Int,Int)]] = edges.flatMap(e=>Array((e.srcId.toInt,e.dstId.toInt)
                                                                    ,(e.dstId.toInt,e.srcId.toInt)))
                                                 .groupBy(f=>f._1).map(f=>(f._1,f._2))
  val nodes = (neighbors.keys).toArray.max+1
  println("inputnode start input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates.size )
  writer.write(edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates.size+"\n")
  var currentCrossEdgesNum = edges.filter(v=>color(v.dstId.toInt)!=color(v.srcId.toInt)).size
  val r = scala.util.Random
  for(counter <- 0 to 350){
    for(currentNode <- 0 to nodes-1){
      var bestNeibour = -1;
      var leastCE = -1;
      val currentNeighbors = neighbors.get(currentNode).get.map(v=>v._2)
      val pp = currentNeighbors.filter(v=>color(v)!=color(currentNode)).size
      currentNeighbors.filter(color(currentNode)!=color(_)).foreach(v=>{
          val qNeibours = neighbors.get(v).get.map(v=>v._2)
          val qq = qNeibours.filter(k=>color(k)!=color(v)).size
          swap(currentNode,v)
          val pq = qNeibours.filter(k=>color(k)!=color(v)).size
          val qp = currentNeighbors.filter(k=>color(k)!=color(currentNode)).size
  //        println("numbers : "+pp+" "+qq+" "+pq+" "+qp)
          if(pp+qq>pq+qp){
            if(leastCE==(-1)||(pp+qq)-(pq+qp)>leastCE){
              bestNeibour = v.toInt
              leastCE = (pp+qq)-(pq+qp)
            }
          }
          swap(currentNode,v)
        } 
      )
      if(bestNeibour==(-1)){
//        println("no swap for "+currentNode)
      }
      else{
        println("swap between "+currentNode+ " and "+bestNeibour+" with counter = "+counter)
        println("before cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) ).size )
        swap(currentNode,bestNeibour)
//        println("reduced cross edges : " + leastCE)
//        println("current cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) ).size )
      }
    }
    writer.write("round : "+counter+" current cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size+"\n")
    println("round : "+counter+" current cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
    println("current input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates.size)
   }
    writer.close()
    println("final cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
    println("final input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates.size)
  }
  def JabeJa_Random(sc:SparkContext):Unit = {
//  val edges = sc.textFile("/home/crazydog/ALIBABA/alibaba.graph.txt", 3).map(line=>{
//                val edge = line.split(" ")
//                Edge(edge(0).toLong,edge(1).toLong,edge(2))
//  }).filter(x=>x.dstId<=26024).collect()
  val writer = new PrintWriter(new File("jabeja-random.round.txt"))
  val edges = sc.textFile(path, 3).map(line=>{
                val edge = line.split(" ")
                Edge(edge(0).toLong,edge(1).toLong,edge(2))
  }).collect()
  val neighbors = edges.flatMap(e=>Array((e.srcId.toInt,e.dstId.toInt)
                                ,(e.dstId.toInt,e.srcId.toInt)))
                                .groupBy(f=>f._1).map(f=>(f._1,f._2.toArray.map(v=>v._2)))
  val nodes = (neighbors.keys).toArray.max+1
  println("inputnode start input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates.size )
  writer.write(edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates.size+"\n")
  var currentCrossEdgesNum = edges.filter(v=>color(v.dstId.toInt)!=color(v.srcId.toInt)).size
  val r = scala.util.Random
  for(counter <- 0 to 350){
    for(currentNode <- 0 to nodes-1){
        var bestNeibour = -1;
        var leastCE = -1.0;
        val currentNeighbors = neighbors.filter(_._1==currentNode).flatMap(v=>v._2).toArray
        var RandomNeighbors : Set[Int]= Set()
        for(x<-1 to edges.size/nodes*2){
           var newNeighbor = scala.util.Random.nextInt.abs%nodes
           if(newNeighbor<0) newNeighbor = newNeighbor+nodes
           while(color(currentNode)==color(newNeighbor)||RandomNeighbors.contains(newNeighbor)==true) {
             newNeighbor = scala.util.Random.nextInt%nodes
             if(newNeighbor<0) newNeighbor = newNeighbor+nodes
           }
           RandomNeighbors.+=(newNeighbor)
        }
        val pp = currentNeighbors.filter(v=>color(v)!=color(currentNode)).size
        RandomNeighbors.filter(color(currentNode)!=color(_)).foreach(v=>{
            val qNeibours = neighbors.filter(_._1==v).flatMap(v=>v._2).toArray
            val qq = qNeibours.filter(k=>color(k)!=color(v)).size
            swap(currentNode,v)
            val pq = qNeibours.filter(k=>color(k)!=color(v)).size
            val qp = currentNeighbors.filter(k=>color(k)!=color(currentNode)).size
            val diff = (pp+qq).toDouble*TEMPERATURE - (pq+qp).toDouble
    //        println("numbers : "+pp+" "+qq+" "+pq+" "+qp)
            if(diff>0){
              if(leastCE==(-1)||diff>leastCE){
                bestNeibour = v.toInt
                leastCE = diff
              }
            }
            swap(currentNode,v)
          } 
        )
        if(bestNeibour==(-1)){
//          println("no swap for "+currentNode)
        }
        else{
          println("swap between "+currentNode+ " and "+bestNeibour+" with counter = "+counter)
    //      println("before cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) ).size )
          swap(currentNode,bestNeibour)
          if(TEMPERATURE-TEMPERATUREDelta>=1) TEMPERATURE = TEMPERATURE-TEMPERATUREDelta
          else TEMPERATURE = 1
//          println("reduced cross edges : " + leastCE)
//          println("current cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
    //      println("current input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
    //                                                 .map(x=>x.dstId).toList.removeDuplicates.size )
        }
      }
      writer.write("round : "+counter+" current cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size+"\n")
      println("round : "+counter+" current cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
      println("current input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates.size)
  }
    writer.close()
    println("final cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
    println("final input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates.size)
  }
  def isInputNode(currentNode : Int,fromNeighborColors: Array[Array[Int]],InDegrees : Array[Int]) : Boolean = {
    return fromNeighborColors(currentNode)(color(currentNode))!=InDegrees(currentNode)
  }
  def updateColor(cur : Int,neigh : Int,fromNeighborColors: Array[Array[Int]],Neighbors : Set[Int]) : Unit = {
      Neighbors.foreach(v=>{
        fromNeighborColors(v)(color(cur)) = fromNeighborColors(v)(color(cur)) +1
        fromNeighborColors(v)(color(neigh)) = fromNeighborColors(v)(color(neigh))-1
      })
  }
  def JabeJa_InputNodeLocal(sc:SparkContext) : Unit = {
    val edges = sc.textFile(path, 3).map(line=>{
                  val edge = line.split(" ")
                  Edge(edge(0).toLong,edge(1).toLong,edge(2))
    }).collect()
    val writer = new PrintWriter(new File("jabeja-inputlocal.round.txt"))
    toNeighbors = edges.map(e=>(e.srcId.toInt,e.dstId.toInt)).groupBy(f=>f._1).map(f=>(f._1,f._2.map(k=>k._2).toSet))
    fromNeighbors= edges.map(e=>(e.dstId.toInt,e.srcId.toInt)).groupBy(f=>f._1).map(f=>(f._1,f._2.map(k=>k._2).toSet))
    val nodes = (toNeighbors.keys++fromNeighbors.keys).toArray.max+1
    var fromNeighborColors = Array.ofDim[Int](nodes,3)
    println("node number : "+nodes)
    var InDegrees : Array[Int] = Array.ofDim[Int](nodes)
    fromNeighbors.foreach(f=>{
      InDegrees(f._1)=f._2.size
      f._2.foreach(v=>fromNeighborColors(f._1)(color(v)) = fromNeighborColors(f._1)(color(v))+1)
    })
//    fromNeighborColors.foreach(println)
    val r = scala.util.Random
    var reduced = 0
    writer.write(List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees)).size+"\n")
    println("inputnode start input nodes : " + List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees)).size )
    for(counter <- 0 to 350){
      for(currentNode <- 0 to nodes-1){
        //      val currentNode = r.nextInt().abs%nodes
        var bestNeibour = -1;
        var leastIN = -1.0;
        val currentNeighbors = toNeighbors.getOrElse(currentNode,Set())
        //if currentnode is inputnode
        currentNeighbors.filter(color(currentNode)!=color(_)).foreach(v=>{
            val vNeibours = toNeighbors.getOrElse(v,Set())
            val pNeibours = currentNeighbors.diff(vNeibours+v)
            val qNeibours = vNeibours.diff(currentNeighbors+currentNode)
            var pinc = pNeibours.filter(k=>isInputNode(k,fromNeighborColors,InDegrees)==false).size
            var pdec = pNeibours.filter(k=>fromNeighborColors(k)(color(k))==(InDegrees(k)-1)
                                                        &&color(k)==color(v)).size
            if(vNeibours.contains(currentNode)==false&&InDegrees(currentNode)>0){
              if(isInputNode(currentNode,fromNeighborColors,InDegrees)==false) pinc = pinc + 1
              else if(fromNeighborColors(currentNode)(color(v))==InDegrees(currentNode)) pdec = pdec + 1
            }
            var qinc = qNeibours.filter(k=>isInputNode(k,fromNeighborColors,InDegrees)==false).size
            var qdec = qNeibours.filter(k=>fromNeighborColors(k)(color(k))==(InDegrees(k)-1)
                                                        &&color(k)==color(currentNode)).size
            if(currentNeighbors.contains(v)==false&&InDegrees(v)>0){
              if(isInputNode(v,fromNeighborColors,InDegrees)==false) qinc = qinc + 1
              else if(fromNeighborColors(v)(color(currentNode))==InDegrees(v)) qdec  = qdec + 1
            }
            val diff = (pdec+qdec).toDouble*TEMPERATURE - (pinc+qinc).toDouble
  //          val diff = (pp+qq) - (pq+qp)
            if(diff>0){
               if(leastIN==(-1)||diff>leastIN){
                  bestNeibour = v.toInt
                  leastIN = diff
               }
            }
         })
         if(bestNeibour==(-1)){
//           println("no swap between "+currentNode+ " and "+bestNeibour+" with counter = "+counter)
         }
         else{
           println("swap between "+currentNode+ " and "+bestNeibour+" with counter = "+counter)
    //          println("before input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
    //                                                 .map(x=>x.dstId).toList.removeDuplicates.size )
           swap(currentNode,bestNeibour)
           updateColor(currentNode,bestNeibour,fromNeighborColors,toNeighbors.getOrElse(currentNode,Set()))
           updateColor(bestNeibour,currentNode,fromNeighborColors,toNeighbors.getOrElse(bestNeibour,Set()))
           println("reduced input nodes : "+leastIN)
           if(TEMPERATURE-TEMPERATUREDelta>=1) TEMPERATURE = TEMPERATURE-TEMPERATUREDelta
           else TEMPERATURE = 1
         }
        }
        writer.write("round : "+counter+"current input nodes : " + List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees)).size+"\n")
        println("round : "+counter+" current cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
        println("current input nodes : " + List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees)).size)
      }
      writer.close()
      println("final input nodes : " + List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees)).size)
      println("final cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
    }
  
  def JabeJa_InputNodeRandom(sc:SparkContext) : Unit = {
    val edges = sc.textFile(path, 3).map(line=>{
                  val edge = line.split(" ")
                  Edge(edge(0).toLong,edge(1).toLong,edge(2))
    }).collect()
    val writer = new PrintWriter(new File("jabeja-inputrandom.round.txt"))
    toNeighbors = edges.map(e=>(e.srcId.toInt,e.dstId.toInt)).groupBy(f=>f._1).map(f=>(f._1,f._2.map(k=>k._2).toSet))
    fromNeighbors= edges.map(e=>(e.dstId.toInt,e.srcId.toInt)).groupBy(f=>f._1).map(f=>(f._1,f._2.map(k=>k._2).toSet))
    val nodes = (toNeighbors.keys++fromNeighbors.keys).toArray.max+1
    var fromNeighborColors = Array.ofDim[Int](nodes,3)
    println("node number : "+nodes)
    var InDegrees : Array[Int] = Array.ofDim[Int](nodes)
    fromNeighbors.foreach(f=>{
      InDegrees(f._1)=f._2.size
      f._2.foreach(v=>fromNeighborColors(f._1)(color(v)) = fromNeighborColors(f._1)(color(v))+1)
    })
    var counter = 0
    val r = scala.util.Random
    var reduced = 0
    val pp = edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
                                                   .map(x=>x.dstId).toList.removeDuplicates
    val qq = List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees))
    writer.write(qq.size+"\n")
    println("edges start input nodes : " + pp.size )
    println("inputnode start input nodes : " + qq.size )
    pp.diff(qq).foreach(println("p-q ",_))
    qq.diff(pp).foreach(println("q-p ",_))
    for(counter <- 0 to 350){
      for(currentNode <- 0 to nodes-1){
  //      val currentNode = 8381 
        var bestNeibour = -1;
        var leastIN = -1.0;
        val currentNeighbors = toNeighbors.getOrElse(currentNode,Set())
        var RandomNeighbors : Set[Int]= Set()
        for(x<-1 to edges.size/nodes*2){
           var newNeighbor = scala.util.Random.nextInt.abs%nodes
           if(newNeighbor<0) newNeighbor = newNeighbor+nodes
           while(color(currentNode)==color(newNeighbor)||RandomNeighbors.contains(newNeighbor)==true) {  
             newNeighbor = scala.util.Random.nextInt%nodes
             if(newNeighbor<0) newNeighbor = newNeighbor+nodes
           }
           RandomNeighbors.+=(newNeighbor)
        }
        //if currentnode is inputnode
        RandomNeighbors
            .foreach(v=>{
            val vNeibours = toNeighbors.getOrElse(v,Set())
            val pNeibours = currentNeighbors.diff(vNeibours+v)
            val qNeibours = vNeibours.diff(currentNeighbors+currentNode)
            var pinc = pNeibours.filter(k=>isInputNode(k,fromNeighborColors,InDegrees)==false).size
            var pdec = pNeibours.filter(k=>fromNeighborColors(k)(color(k))==(InDegrees(k)-1)
                                                        &&color(k)==color(v)).size
            if(vNeibours.contains(currentNode)==false&&InDegrees(currentNode)>0){
              if(isInputNode(currentNode,fromNeighborColors,InDegrees)==false) pinc = pinc + 1
              else if(fromNeighborColors(currentNode)(color(v))==InDegrees(currentNode)) pdec = pdec + 1
            }
            var qinc = qNeibours.filter(k=>isInputNode(k,fromNeighborColors,InDegrees)==false).size
            var qdec = qNeibours.filter(k=>fromNeighborColors(k)(color(k))==(InDegrees(k)-1)
                                                        &&color(k)==color(currentNode)).size
            if(currentNeighbors.contains(v)==false&&InDegrees(v)>0){
              if(isInputNode(v,fromNeighborColors,InDegrees)==false) qinc = qinc + 1
              else if(fromNeighborColors(v)(color(currentNode))==InDegrees(v)) qdec  = qdec + 1
            }
            val diff = (pdec+qdec).toDouble*TEMPERATURE - (pinc+qinc).toDouble
  //          val diff = (pp+qq) - (pq+qp)
            if(diff>0){
               if(leastIN==(-1)||diff>leastIN){
                  bestNeibour = v.toInt
                  leastIN = diff
               }
            }
         })
         if(bestNeibour==(-1)){
//           println("no swap between "+currentNode+ " and "+bestNeibour+" with counter = "+counter)
         }
         else{
           println("swap between "+currentNode+ " and "+bestNeibour+" with counter = "+counter)    //          println("before input nodes : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt) )
    //                                                 .map(x=>x.dstId).toList.removeDuplicates.size )
           swap(currentNode,bestNeibour)
           updateColor(currentNode,bestNeibour,fromNeighborColors,toNeighbors.getOrElse(currentNode,Set()))
           updateColor(bestNeibour,currentNode,fromNeighborColors,toNeighbors.getOrElse(bestNeibour,Set()))
           if(TEMPERATURE-TEMPERATUREDelta>=1) TEMPERATURE = TEMPERATURE-TEMPERATUREDelta
           else TEMPERATURE = 1
         }
        }
        writer.write("round : "+counter+"current input nodes : " + List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees)).size+"\n")
        println("round : "+counter+" current cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
        println("current input nodes : " + List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees)).size)
      }
      writer.close()
      println("final input nodes : " + List.range(0,nodes).filter(isInputNode(_,fromNeighborColors,InDegrees)).size)
      println("final cross edges : " + edges.filter( e=>color(e.dstId.toInt)!=color(e.srcId.toInt)).size )
  }
  def main(args:Array[String]) = {
    path  = args(0)
    sparkMaster = args(1)  
    val output_path = args(2)
    val function = args(3)
      val sparkConf = new SparkConf().setAppName("JabeJa : ").setMaster(sparkMaster)
      val sc = new SparkContext(sparkConf)
//    JabeJa_Local
//    JabeJa_Random
    var initColor = (List.fill(17032)(0)++(List.fill(17326)(1))++(List.fill(17692)(2)))
//    var initColor = (List.fill(4068)(0)++(List.fill(4204)(1))++(List.fill(4163)(2)))
//    var initColor = (List.fill(4973)(0)++(List.fill(5118)(1))++(List.fill(4997)(2)))
//    var initColor = (List.fill(9)(0)++(List.fill(9)(1))++(List.fill(8)(2)))
//    color = initColor.toArray
    color = scala.util.Random.shuffle(initColor).toArray
    if(function=="edgelocal"){
      println("JabeJa_Local!")
      JabeJa_Local(sc) 
    }
    if(function=="edgerandom"){
      println("JabeJa_Random!")
      JabeJa_Random(sc) 
    }
    if(function=="inputlocal"){
      println("JabeJa_InputNodeLocal!")
      JabeJa_InputNodeLocal(sc)
    }
    if(function=="inputrandom"){
      println("JabeJa_InputNodeRandom!")
      JabeJa_InputNodeRandom(sc)
    }
      
//    JabeJa_InputNodeLocal(color,sc)
//    JabeJa_InputNodeRandom(sc)
//    fromNeighborColors.foreach(println)
    val writer = new PrintWriter(new File(output_path))
    color.foreach(v=>writer.write(v+"\n"))
    writer.close()
  }
}