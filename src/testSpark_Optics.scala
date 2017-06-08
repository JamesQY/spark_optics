/**
  * Created by Administrator on 2016/6/22.
  */

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import spark_optics._

import scala.collection.mutable.ArrayBuffer

object testSpark_Optics {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testSpark_Optics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val dataSet: List[List[Double]] = List(List(1, 3, 3), List(2, 3, 3), List(3, 3, 3), List(1, 2, 3), List(2, 2, 3), List(3, 2, 3), List(1, 1, 3), List(2, 1, 3)
      , List(3, 1, 3), List(7, 7, 0), List(8, 7, 0), List(9, 7, 0), List(7, 6, 0), List(8, 6, 0), List(9, 6, 0), List(7, 5, 0), List(8, 5, 0), List(9, 5, 0), List(23, 1, 2), List(24, 1, 2)
      , List(24, 2, 2), List(25, 1, 2), List(25, 2, 2), List(26, 2, 2), List(26, 1, 2), List(46, 11, 1), List(46, 12, 1), List(47, 11, 1), List(47, 12, 1), List(47, 13, 1), List(48, 13, 1), List(48, 12, 1)
      , List(48, 11, 1), List(49, 12, 1), List(49, 13, 1), List(50, 12, 1), List(50, 13, 1), List(51, 12, 1), List(51, 13, 1), List(51, 14, 1), List(52, 11, 1), List(52, 12, 1), List(52, 13, 1),
      List(52, 14, 1), List(53, 12, 1), List(98, 3, 4), List(99, 3, 4), List(99, 2, 4), List(99, 1, 4), List(100, 1, 4), List(100, 2, 4), List(100, 3, 4),
      List(100, 10, 4), List(47, 1, -1), List(5, 4, -1), List(7, 10, 0), List(8, 10, 0), List(8, 9, 0), List(9, 9, 0), List(8, 8, 0), List(8, 4, 0), List(9, 4, 0),
      List(8, 3, 0), List(9, 3, 0), List(8, 2, 0), List(9, 2, 0), List(7, 2, 0), List(25, 3, 2), List(25, 4, 2), List(24, 4, 2), List(25, 5, 2), List(26, 5, 2))
    val pointArray = ArrayBuffer[Point]()
    var i = 0
    for (x <- dataSet) {
      val tempPoint = new Point(i, x.take(2))
      tempPoint.sourceClusterID = x(2).toInt
      pointArray += tempPoint
      i = i + 1
    }
    val pointrdd = sc.parallelize(pointArray, 10)
    pointrdd.cache()
    val opticsSettings = new OpticsSettings()
    val partitioningSettings = new PartitioningSettings(5)

    val spark_optics = new Spark_Optics1(opticsSettings, partitioningSettings)
    val opticsModel = spark_optics.run(pointrdd)

    val write = new PrintWriter(new FileWriter("./src/result/testSpark_OpticsResult.csv"))
    write.println(opticsModel.runningTime)
    val result = opticsModel.getDetailResult()
    //    val result=opticsModel.getResult()
    //    sc.parallelize(result,60).sortBy(_._1).saveAsTextFile("./src/result")
    for (elem <- result)
      write.println(elem.toString())
    write.close()
    sc.stop()
  }
}
