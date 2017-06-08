package spark_optics

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by Administrator on 2016/6/22.
  */
class Spark_Optics
(settings: OpticsSettings,
 partitioningSettings: PartitioningSettings = new PartitioningSettings()) extends Serializable {


  def run(data: RDD[Point]): OpticsModel = {

    val startTime = System.currentTimeMillis()
    val sc = data.sparkContext
    val analyer = new Analyzer(settings, partitioningSettings)
    val blockCalculator = new BlockCalculator1(data.cache())
    val (blocks, dataPoint) = blockCalculator.generateDensityBasedBLocks(partitioningSettings, settings)
    val blocklength = blocks.length
    val broadcastBlocks = sc.broadcast(blocks)
    val PointRDD = sc.parallelize(dataPoint).zipWithIndex.partitionBy(new BlockPartitioner(blocks.length))
      .map(x => x._1).cache()

    val calculateNighborRDD = PointRDD.mapPartitionsWithIndex((partitionIndex, it) => {
      val blocks = broadcastBlocks.value //计算邻居
      var partitionBeforeBlock: Block = null
      if (partitionIndex != 0)
        partitionBeforeBlock = blocks.find(_.blockID == partitionIndex - 1).get
      val partitionBlock = blocks.find(_.blockID == partitionIndex).get
      var partitionAfterBlock: Block = null
      if (partitionIndex != blocks.length - 1)
        partitionAfterBlock = blocks.find(_.blockID == partitionIndex + 1).get

      val it1 = it.map { x => (x, partitionBlock.PointArray.map
      (j => (analyer.calculateEuclidDistanceInPartition(j, x, 0))))
      }
        .map { x =>
          var tempPartitionBeforeBlock = x._2
          if (x._1.isBoundary == -1 && partitionBeforeBlock != null)
            tempPartitionBeforeBlock ++= partitionBeforeBlock.PointArray.filter(_.isBoundary == 1).
              map(j => analyer.calculateEuclidDistanceInPartition(j, x._1, -1))
          (x._1, tempPartitionBeforeBlock)
        }
        .map { x =>
          var tempPartitionAfterBlock = x._2
          if (x._1.isBoundary == 1 && partitionAfterBlock != null)
            tempPartitionAfterBlock ++= partitionAfterBlock.PointArray.filter(_.isBoundary == -1).
              map(j => analyer.calculateEuclidDistanceInPartition(j, x._1, 1))
          (x._1, tempPartitionAfterBlock)
        }
      it1
    }, preservesPartitioning = true)
      .map(x => (x._1, x._2.filter(_._1 <= settings.epsilon).sortBy(_._1)))
      .map { x =>
        var pt = x._1 //得到邻居数组
        pt.NighborArray = x._2
        pt
      }.map { x =>
      var pt = x //赋予核心距离
      if (pt.NighborArray.length >= settings.numPoints)
        pt.CoreDistance = pt.NighborArray(settings.numPoints - 1)._1
      pt
    }.cache()

    val testcalculateNighborRDD = calculateNighborRDD.collect()
    println(testcalculateNighborRDD.length)


    val PartitionOptics = calculateNighborRDD.mapPartitionsWithIndex((partitionIndex, it) => {
      val itArray = it.toArray //执行optics算法
      val resultArray = ArrayBuffer[Point]()
      val seedArray = ArrayBuffer[Point]()
      var TempClusterID = 0
      for (itElem <- itArray) {
        var tempPoint = itElem
        if (!tempPoint.isVisit) {
          tempPoint.isVisit = true
          resultArray += tempPoint

          if (tempPoint.CoreDistance != -1.0) {
            tempPoint.ClusterID = (partitionIndex, TempClusterID)
            for (i <- 1 until tempPoint.NighborArray.length) {
              if (tempPoint.NighborArray(i)._3 == 0) {
                val nighborPoint = itArray.find(_.pointID == tempPoint.NighborArray(i)._2).get
                if (!nighborPoint.isVisit) {
                  nighborPoint.ReachDistance = math.max(tempPoint.CoreDistance, tempPoint.NighborArray(i)._1)
                  seedArray += nighborPoint
                }
              }
            }
            seedArray.sortBy(_.ReachDistance)
            while (!seedArray.isEmpty) {
              val nextPoint = seedArray.remove(0)
              nextPoint.isVisit = true
              nextPoint.ClusterID = (partitionIndex, TempClusterID)
              resultArray += nextPoint
              if (nextPoint.CoreDistance != -1.0) {
                for (i <- 1 until nextPoint.NighborArray.length) {
                  if (nextPoint.NighborArray(i)._3 == 0) {
                    val nighborPoint = itArray.find(_.pointID == nextPoint.NighborArray(i)._2).get
                    if (!nighborPoint.isVisit) {
                      if (!seedArray.contains(nighborPoint)) {
                        nighborPoint.ReachDistance = math.max(nextPoint.CoreDistance, nextPoint.NighborArray(i)._1)
                        seedArray += nighborPoint
                      }
                      else {
                        val tempPoint = seedArray.find(_.pointID == nighborPoint.pointID).get
                        if (tempPoint.ReachDistance > nextPoint.NighborArray(i)._1)
                          tempPoint.ReachDistance = nextPoint.NighborArray(i)._1
                      }
                    }
                  }
                }
                seedArray.sortBy(_.ReachDistance)
              }
            }
            TempClusterID += 1
          }

        }
      }
      itArray.toIterator
    }, preservesPartitioning = true).cache()

    val testPartitionOptics = PartitionOptics.collect()
    println(testPartitionOptics.length)

    val TempPartitionOptics = PartitionOptics.filter(_.isBoundary != 0).filter(_.NighborArray.length != 1).cache()

    val findMap = TempPartitionOptics.map(x => (x.pointID, x.ClusterID, x.BlockID, x.isBoundary, x.NighborArray)).collect()
    val broadFindMap = sc.broadcast(findMap)
    val accumulatorMap = sc.accumulator(Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)]())(MyAccumulatorParam)

    val generateMaping = TempPartitionOptics.mapPartitionsWithIndex((partitionIndex, it) => {
      var tempMap = Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)]() //只判断那些边界点
      val FindMap = broadFindMap.value
      val it1 = it.toArray
      for (x <- it1) {
        if (!(partitionIndex == 0 && x.isBoundary == -1) && !(partitionIndex == blocklength - 1 && x.isBoundary == 1)) {
          if (x.isBoundary == -1) //是这个分区前边界的点
          {
            for (j <- FindMap) {
              if (j._4 == 1 && j._3 == partitionIndex - 1) //在map里面找到前面分区后面的点
              {
                breakable {
                  for (k <- j._5) {
                    if (k._3 == 1 && k._2 == x.pointID) //这两个点是邻居
                    {
                      if (j._2 != (-1, -1)) //如果前面分区的点不是噪声点
                      {
                        if (x.ClusterID != (-1, -1)) //这个分区的点不是噪声点
                        {
                          if (!tempMap.contains((x.BlockID, x.ClusterID, 0))) {
                            tempMap += (x.BlockID, x.ClusterID, 0) -> (j._3, j._2, 0)
                            break()
                          }
                          else {
                            //如果包含这个key，就把这个值的赋给前面的map
                            if ((j._3, j._2, 0) != tempMap((x.BlockID, x.ClusterID, 0)))
                              tempMap += (j._3, j._2, 0) -> tempMap((x.BlockID, x.ClusterID, 0))
                          }
                        }
                        else if (x.ClusterID == (-1, -1)) //这个分区的点是噪声点
                        {
                          if (!tempMap.contains(x.BlockID, (-1, -1), x.pointID)) {
                            tempMap += (x.BlockID, (-1, -1), x.pointID) -> (j._3, j._2, 0)
                            break()
                          }
                        }
                      }
                      else //如果前面分区的点是噪声点
                      {
                        if (x.ClusterID != (-1, -1)) //这个分区的点不是噪声点
                        {
                          if (!tempMap.contains((j._3, (-1, -1), j._1))) {
                            tempMap += (j._3, (-1, -1), j._1) -> (x.BlockID, x.ClusterID, 0) //这时候就以噪声点为key
                            break()
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      accumulatorMap += tempMap
      it1.toIterator
    }, preservesPartitioning = true).filter(_.pointID == -1).collect()


    val AllMap = accumulatorMap.value
    //    println(AllMap)

    val broadAllMap = sc.broadcast(AllMap)

    val resignRDD = PartitionOptics.map { x => //合并簇号
      val tempMap = broadAllMap.value
      var PointMap: Triple[Int, (Int, Int), Int] = null
      var finallyPointMap: Triple[Int, (Int, Int), Int] = null
      if (x.ClusterID != (-1, -1)) {
        PointMap = (x.BlockID, x.ClusterID, 0)
      }
      else {
        PointMap = (x.BlockID, x.ClusterID, x.pointID)
      }
      if (tempMap.contains(PointMap)) {
        finallyPointMap = tempMap(PointMap) //如果有更多的map，查找到最前面分区的簇号，并赋予它
        while (tempMap.contains(finallyPointMap)) {
          finallyPointMap = tempMap(finallyPointMap)
        }
        x.ClusterID = finallyPointMap._2
      }
      x
    }.cache()

    //    val testArray=resignRDD.collect()
    //    println(testArray.length)
    //    println(new spark_optics.Analyzer(settings).getEveryPartitionItemNumber(generateMaping).mkString("",",","")+"...."+generateMaping.partitions.size)
    val endTime = System.currentTimeMillis()
    //    println(endTime-startTime)

    new OpticsModel(resignRDD, settings, endTime - startTime)
  }

}
