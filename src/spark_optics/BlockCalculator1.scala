package spark_optics

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


/**
  * Created by Administrator on 2016/6/24.
  */
class BlockCalculator1(val data: RDD[Point]) extends Serializable {
  var opticsSettings: OpticsSettings = new OpticsSettings()
  var partitioningSettings: PartitioningSettings = new PartitioningSettings()
  var analyzer: Analyzer = new Analyzer(opticsSettings, partitioningSettings)

  def generateDensityBasedBLocks(partitioningSettings: PartitioningSettings = new PartitioningSettings(),
                                 opticsSettings: OpticsSettings = new OpticsSettings()): (List[Block], Array[Point]) = {
    this.opticsSettings = opticsSettings //生成分区数组
    this.partitioningSettings = partitioningSettings
    analyzer = new Analyzer(opticsSettings, partitioningSettings)

    val (locationLargeDeminsion, largeDeminsionValueMinValue, largeDeminsionValueMaxValue)
    = analyzer.calculateLagerestDeminsion(data)

    val acc = data.sparkContext.accumulator(Array[Point]())(ArrayAccumulator)
    val accum = data.sparkContext.accumulator(0)

    //    val datanum=data.count().toInt

    val data1 = data.map { x =>
      x.LongestDimensionvalues = x.PointList(locationLargeDeminsion)
      x
    }.sortBy(_.LongestDimensionvalues)
      .repartition(1).mapPartitions { x =>
      val BlockList = ArrayBuffer[Block]()
      val x1 = x.toArray
      val RootNode = generateBlockTree(largeDeminsionValueMinValue, largeDeminsionValueMaxValue, x1)
      Pre_OrderBlockTree(RootNode, BlockList)
      BlockList.toIterator
    }.collect()

    val datacount = data.sparkContext.parallelize(data1, 1).mapPartitions { x =>
      accum += x.length
      x
    }.filter(_.blockID == -1).collect()
    val accumvalue = accum.value

    val data2 = data.sparkContext.parallelize(data1, 1).zipWithIndex.partitionBy(new BlockPartitioner(accumvalue)).map(x => x._1)
      .map { x =>
        val temp = x
        //      val num=datanum/accumvalue
        //
        //      if(temp.PointArray.length>2*num)
        //        temp.PointArray=sample(temp.PointArray.toArray,temp.PointArray.length,(temp.PointArray.length*0.7).toInt)
        temp.PointArray.map {
          k =>
            k.blockLength = accumvalue
            k.BlockID = temp.blockID
            if (k.LongestDimensionvalues - temp.lowerBound < opticsSettings.epsilon)
              k.isBoundary = -1
            else if (temp.higherBound - k.LongestDimensionvalues <= opticsSettings.epsilon)
              k.isBoundary = 1
            k
        }
        acc ++= temp.PointArray.toArray
        temp
      }.collect()

    (data2.toList, acc.value)
  }


  def generateBlockTree(lowerBound: Double, higherBound: Double, PointArray: Array[Point]): BlockTreeNode = {
    val block: Block = new Block()
    block.lowerBound = lowerBound //生成一颗树
    block.higherBound = higherBound
    val Node = new BlockTreeNode(block)
    breakable {
      while (((block.higherBound - block.lowerBound) > 2 * opticsSettings.epsilon) && (PointArray.size > partitioningSettings.numberOfPointsInBlock)) {
        val i = analyzer.getHigherBoundLowerBoundHalf(PointArray, (block.higherBound + block.lowerBound) / 2)
        if (i != PointArray.length && i != 0) {
          Node.leftChildren = generateBlockTree(block.lowerBound, (block.higherBound + block.lowerBound) / 2, PointArray.take(i))
          Node.rightChildren = generateBlockTree((block.higherBound + block.lowerBound) / 2, block.higherBound, PointArray.takeRight(PointArray.length - i))
          break()
        }
        else if (i == PointArray.length) {
          block.higherBound = (block.higherBound + block.lowerBound) / 2
        }
        else if (i == 0) {
          block.lowerBound = (block.higherBound + block.lowerBound) / 2
        }
      }
    }
    if (Node.leftChildren == null && Node.rightChildren == null)
      block.PointArray ++= PointArray
    return Node
  }

  def Pre_OrderBlockTree(root: BlockTreeNode, BlockList: ArrayBuffer[Block]): Unit = {

    if (root.leftChildren != null) //先序遍历
      Pre_OrderBlockTree(root.leftChildren, BlockList)
    if (root.leftChildren == null && root.rightChildren == null) {
      root.block.blockID = BlockCalculator1.DefalutblockID
      BlockCalculator1.DefalutblockID += 1
      BlockList += root.block
    }
    if (root.rightChildren != null)
      Pre_OrderBlockTree(root.rightChildren, BlockList)
  }

  def sample(arr: Array[Point], length: Int, num: Int): ArrayBuffer[Point] = {
    val randomList = List.fill(num)(scala.util.Random.nextInt(length))
    val arrayBuffer = ArrayBuffer[Point]()
    for (x <- randomList) {
      arrayBuffer += arr(x)
    }
    arrayBuffer
  }
}


object BlockCalculator1 extends Serializable {
  var DefalutblockID: Int = 0
}
