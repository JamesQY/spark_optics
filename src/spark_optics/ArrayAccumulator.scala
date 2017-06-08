package spark_optics

import org.apache.spark.AccumulatorParam

/**
  * Created by Administrator on 2016/7/14.
  */
object ArrayAccumulator extends AccumulatorParam[Array[Point]] {
  def zero(initialValue: Array[Point]): Array[Point] = {
    Array[Point]()
  }

  def addInPlace(v1: Array[Point], v2: Array[Point]): Array[Point] = {
    v1 ++ v2
  }
}
