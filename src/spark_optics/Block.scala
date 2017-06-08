package spark_optics

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/6/22.
  */
class Block(var blockID: Int = 0) extends Serializable {
  var resultVertor: List[Point] = List()
  var partitionID: Int = -1
  var lowerBound: Double = 0.0
  var higherBound: Double = 0.0
  var PointArray = ArrayBuffer[Point]()
}
