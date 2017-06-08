package spark_optics

import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2016/7/4.
  */
class OpticsModel(val resultData: RDD[Point], val settings: OpticsSettings = new OpticsSettings(),
                  val runningTime: Double) //进行简历model
  extends Serializable {


  def predict(newPoint: Point): Int = {
    val analyer = new Analyzer(settings)
    val neighborCountsByCluster = resultData.map(x => (x.finallyClusterID, analyer.calculateEuclidDistanceInPartition(x, newPoint)))
      .filter(_._2 < settings.epsilon).map(x => (x._1, 1)).reduceByKey(_ + _).collect()

    val neighborCountsWithoutNoise = neighborCountsByCluster.filter(_._1 != -1)
    val possibleClusters = neighborCountsWithoutNoise.filter(_._2 >= settings.numPoints - 1)
    var noisePointCount = 0
    if (neighborCountsByCluster.filter(_._1 == -1).length > 0)
      noisePointCount = neighborCountsByCluster.filter(_._1 == -1)(0)._2

    if (possibleClusters.size >= 1) {
      //如果在很多可能簇中间，返回最可能的一个
      possibleClusters.head._1
    }
    else if (neighborCountsWithoutNoise.size >= 1) {
      //如果在几个可能簇的边缘，就返回最可能的一个
      neighborCountsWithoutNoise.head._1
    }
    else if (noisePointCount >= settings.numPoints) {
      -2 //新的簇
    }
    else {
      -1 //单独的噪声
    }

  }

  def getResult(): Array[(Int, Int, Int)] = {
    val result = resultData.map(x => (x.pointID, x.sourceClusterID, x.finallyClusterID)).sortBy(_._1).collect()
    result
  }

  def getMap(): Map[Int, (Int, Int)] = {
    val result = resultData.map(x => (x.pointID, x.sourceClusterID, x.finallyClusterID)).keyBy(_._1)
      .map(x => (x._1, (x._2._2, x._2._3))).sortBy(_._1).collect().toMap
    result
  }


  def getDetailResult(): Array[(Int, List[Double], Int, Int)] = {
    val result = resultData.map(x => (x.pointID, x.PointList, x.sourceClusterID, x.finallyClusterID))
      .sortBy(_._1).collect()
    result
  }
}
