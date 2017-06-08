package spark_optics

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/6/22.
  */
class Point
(val pointID: Int,
 val PointList: List[Double],
 var ClusterID: (Int, Int) = (-1, -1),
 var CoreDistance: Double = -1.0,
 var ReachDistance: Double = -1.0,
 var isBoundary: Int = 0,
 var BlockID: Int = -1,
 var LongestDimensionvalues: Double = 0.0,
 var isVisit: Boolean = false
) extends Serializable with Ordered[Point] {
  var finallyClusterID = -1
  var sourceClusterID = -1
  var NighborArray = ArrayBuffer[(Double, Int, Int)]()
  var blockLength: Int = -1

  def this(pt: Point) = this(pt.pointID, pt.PointList, pt.ClusterID, pt.CoreDistance, pt.ReachDistance,
    pt.isBoundary, pt.BlockID, pt.LongestDimensionvalues)

  override def compare(that: Point): Int = {
    if (this.LongestDimensionvalues > that.LongestDimensionvalues) {
      1
    }
    else if (this.LongestDimensionvalues < that.LongestDimensionvalues) {
      -1
    }
    else {
      0
    }
  }
}
