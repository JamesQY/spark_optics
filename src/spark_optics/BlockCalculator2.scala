package spark_optics

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


/**
  * Created by Administrator on 2016/6/24.
  */
class BlockCalculator2(val data: RDD[Point]) extends Serializable {
  var opticsSettings: OpticsSettings = new OpticsSettings()
  var partitioningSettings: PartitioningSettings = new PartitioningSettings()
  var TempBlockList = ArrayBuffer[Block]()
  var analyzer: Analyzer = new Analyzer(opticsSettings, partitioningSettings)
  var dataArray = ArrayBuffer[Point]()

  def generateDensityBasedBLocks(partitioningSettings: PartitioningSettings = new PartitioningSettings(),
                                 opticsSettings: OpticsSettings = new OpticsSettings()): (List[Block], Array[Point]) = {
    this.opticsSettings = opticsSettings //生成分区数组
    this.partitioningSettings = partitioningSettings
    analyzer = new Analyzer(opticsSettings, partitioningSettings)

    val (locationLargeDeminsion, largeDeminsionValueMinValue, largeDeminsionValueMaxValue)
    = analyzer.calculateLagerestDeminsion(data)

    val data1 = data.map { x =>
      x.LongestDimensionvalues = x.PointList(locationLargeDeminsion)
      x
    }.sortBy(_.LongestDimensionvalues).collect
    dataArray ++= data1

    val RootNode = generateBlockTree(largeDeminsionValueMinValue, largeDeminsionValueMaxValue, 0, dataArray.length)
    Pre_OrderBlockTree(RootNode)

    val ListBlock = TempBlockList.toList
    var dataPoint = ArrayBuffer[Point]()
    for (j <- 0 until ListBlock.length) {
      ListBlock(j).blockID = j
      val tempList = data.sparkContext.parallelize(ListBlock(j).PointArray, 60).map { x =>
        x.BlockID = j
        if (x.LongestDimensionvalues - ListBlock(j).lowerBound < opticsSettings.epsilon)
          x.isBoundary = -1
        else if (ListBlock(j).higherBound - x.LongestDimensionvalues <= opticsSettings.epsilon)
          x.isBoundary = 1
        x
      }.collect()
      dataPoint ++= tempList
      ListBlock(j).PointArray.clear()
      ListBlock(j).PointArray ++= tempList
    }
    (ListBlock, dataPoint.toArray)
  }


  def generateBlockTree(lowerBound: Double, higherBound: Double, lowerIndex: Int, higherIndex: Int): BlockTreeNode = {
    val block: Block = new Block()
    block.lowerBound = lowerBound //生成一颗树
    block.higherBound = higherBound

    val PointArray = dataArray.take(higherIndex).takeRight(higherIndex - lowerIndex).toArray
    val Node = new BlockTreeNode(block)

    breakable {
      while (((block.higherBound - block.lowerBound) > 4 * opticsSettings.epsilon) && (PointArray.size > partitioningSettings.numberOfPointsInBlock)) {
        val iInit = analyzer.getHigherBoundLowerBoundHalf(PointArray, (block.higherBound + block.lowerBound) / 2)
        if (iInit != PointArray.length && iInit != 0) {
          val i = analyzer.getHigherBoundLowerBoundHalf(dataArray.toArray, (block.higherBound + block.lowerBound) / 2)
          Node.leftChildren = generateBlockTree(block.lowerBound, (block.higherBound + block.lowerBound) / 2, lowerIndex, i)
          Node.rightChildren = generateBlockTree((block.higherBound + block.lowerBound) / 2, block.higherBound, i, higherIndex)
          break()
        }
        else if (iInit == PointArray.length) {
          block.higherBound = (block.higherBound + block.lowerBound) / 2
        }
        else if (iInit == 0) {
          block.lowerBound = (block.higherBound + block.lowerBound) / 2
        }
      }
    }
    if (Node.leftChildren == null && Node.rightChildren == null)
      block.PointArray ++= PointArray

    return Node
  }

  def Pre_OrderBlockTree(root: BlockTreeNode): Unit = {

    if (root.leftChildren != null) //先序遍历
      Pre_OrderBlockTree(root.leftChildren)
    if (root.leftChildren == null && root.rightChildren == null) {
      root.block.blockID = BlockCalculator.DefalutblockID
      BlockCalculator2.DefalutblockID += 1
      TempBlockList += root.block
    }
    if (root.rightChildren != null)
      Pre_OrderBlockTree(root.rightChildren)
  }

}

object BlockCalculator2 extends Serializable {
  var DefalutblockID: Int = 0

}
