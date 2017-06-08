package spark_optics

import org.apache.spark.AccumulatorParam

/**
  * Created by Administrator on 2016/7/2.
  */
object MyAccumulatorParam extends AccumulatorParam[Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)]] {
  def zero(initialValue: Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)]): Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)] = {
    Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)]()
  }

  def addInPlace(v1: Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)], v2: Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)]): Map[(Int, (Int, Int), Int), (Int, (Int, Int), Int)] = {
    v1 ++ v2
  }
}
