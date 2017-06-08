package spark_optics

import org.apache.spark.Partitioner

/**
  * Created by Administrator on 2016/7/20.
  */
class NewBlockPartitioner(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts

  def getPartition(key: Any): Int = {
    key match {
      case pt: Point => pt.BlockID + scala.util.Random.nextInt(10) * pt.blockLength
      //        case block:Block=>block.blockID
      case _ => 0
    }
  }
}
