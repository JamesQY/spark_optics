package spark_optics

import org.apache.spark.rdd.RDD


/**
  * Created by Administrator on 2016/6/22.
  */

import scala.util.control.Breaks._

class Analyzer
(val settings: OpticsSettings,
 val partitioningSettings: PartitioningSettings = new PartitioningSettings())
  extends Serializable {

  //  var dataMinList:ArrayBuffer[Double]=ArrayBuffer[Double]()
  //  var dataMaxList:ArrayBuffer[Double]=ArrayBuffer[Double]()\

  def getNumberOfDeminsion(data: RDD[Point]): Int = //得到列数
  {
    val pt = data.first()
    pt.PointList.length
  }

  def calculateLagerestDeminsion(data: RDD[Point]): (Int, Double, Double) = //得到最长的维度，最大值，最小值
  {
    data.cache()
    val numberOfDeminsion = getNumberOfDeminsion(data)
    val dataMin = data.map(x => x.PointList)
      .reduce
      //      .fold(List.fill(numberOfDeminsion)(Double.MaxValue))
    {
      (List1, List2) => (List1.zip(List2).map(x => Math.min(x._1, x._2)))
    }
    val dataMax = data.map(x => x.PointList)
      //      .fold(List.fill(numberOfDeminsion)(Double.MinValue))
      .reduce {
      (List1, List2) => (List1.zip(List2).map(x => Math.max(x._1, x._2)))
    }
    val MaxMinDeminsion = dataMin.zip(dataMax).map(x => x._2 - x._1)
    var LagerestDeminsion = 0
    for (i <- 0 until MaxMinDeminsion.length) {
      if (MaxMinDeminsion(i) == MaxMinDeminsion.max)
        LagerestDeminsion = i
    }
    val minValue = dataMin(LagerestDeminsion)
    //    dataMinList++=dataMin
    val maxValue = dataMax(LagerestDeminsion)
    //    dataMaxList++=dataMax
    (LagerestDeminsion, minValue, maxValue)
  }


  def getHigherBoundLowerBoundHalf(data: Array[Point], HigherBoundLowerBoundHalf: Double): Int = //得到划分数组的坐标
  {
    var i = -1
    breakable {
      for (j <- 0 until data.length) {
        if (data(j).LongestDimensionvalues > HigherBoundLowerBoundHalf) {
          i = j
          break()
        }
      }
    }
    if (i == -1) i = data.length
    i
  }

  def calculateEuclidDistanceInPartition(point1: Point, point2: Point): Double = //计算欧式距离
  {
    val EuclidDistance = math.sqrt(point1.PointList.zip(point2.PointList).map(x => math.pow((x._1 - x._2), 2)).sum)
    EuclidDistance
  }

  def calculateEuclidDistanceInPartition(point1: Point, point2: Point, PartitionLocation: Int): (Double, Int, Int) = {
    val EuclidDistance = math.sqrt(point1.PointList.zip(point2.PointList).map(x => math.pow((x._1 - x._2), 2)).sum)
    //      val EuclidDistance=new EuclideanDistance ().compute(point1.PointList.toArray,point2.PointList.toArray)
    (EuclidDistance, point1.pointID, PartitionLocation)
  }


  //  def calculateEuclidDistanceInPartition(point1: spark_optics.Point,point2: spark_optics.Point,PartitionLocation:Int):(Double,Int,Int)=
  //  {
  //    val EuclidDistance=math.sqrt(point1.PointList.zip(point2.PointList).zip(dataMinList).zip(dataMaxList)
  //      .map{x =>
  //      val cDistance=(x._1._1._1 - x._1._1._2)/(x._2-x._1._2)
  //      math.pow(cDistance, 2)
  //    }.sum)
  //
  //    (EuclidDistance,point1.pointID,PartitionLocation)
  //  }


  def getEveryPartitionItemNumber(data: RDD[Point]): Array[(String, Int)] = //可以查看每个分区的点数量
  {
    val EveryPartitionItemNumber = data.mapPartitionsWithIndex {
      (partIdx, iter) => {
        val part_map = scala.collection.mutable.Map[String, Int]()

        while (iter.hasNext) {
          val part_name = "part_" + partIdx
          if (part_map.contains(part_name)) {
            val ele_cnt = part_map(part_name)
            part_map(part_name) = ele_cnt + 1
          } else {
            part_map(part_name) = 1
          }
          iter.next()
        }
        part_map.iterator
      }
    }.collect()
    EveryPartitionItemNumber
  }


}

