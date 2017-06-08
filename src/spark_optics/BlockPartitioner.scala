package spark_optics

import org.apache.spark.Partitioner

/**
  * Created by Administrator on 2016/6/22.
  */
class BlockPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  def getPartition(key: Any): Int = {
    key match {
      case pt: Point => pt.BlockID
      case block: Block => block.blockID
      case _ => 0
    }
  }

  //  def getPartition(key: Any): Int = {
  //    if(key==1)
  //      0
  //    else
  //      1
  //  }
}
