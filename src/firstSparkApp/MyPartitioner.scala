package firstSparkApp
import org.apache.spark.Partitioner
class MyPartitioner[V](
    partitions: Int,
    elements: Int)
  extends Partitioner {
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    // `k` is assumed to go continuously from 0 to elements-1.
    return (k % partitions).toInt
  }
  def numPartitions: Int = partitions
}