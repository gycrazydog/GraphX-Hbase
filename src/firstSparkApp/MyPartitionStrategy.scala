package firstSparkApp
import org.apache.spark.graphx._

object MyPartitionStrategy extends PartitionStrategy {
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID =  if(src!=2) 1 else 2
}