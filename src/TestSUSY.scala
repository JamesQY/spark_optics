import org.apache.spark.{SparkConf, SparkContext}
import spark_optics.{OpticsSettings, PartitioningSettings, Point, Spark_Optics1}

import scala.collection.mutable.ArrayBuffer

//import java.io.PrintWriter

/**
  * Created by Administrator on 2016/7/6.
  */
object TestSUSY {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testSpark_Optics").set("spark.akka.frameSize", "2047")
      .set("spark.driver.maxResultSize", "4096m")
    val sc = new SparkContext(conf)

    val dataSetArray = sc.textFile(args(0), 100).map(x => x.split(",", -1).map(_.toDouble)).zipWithIndex()
      .map { x =>
        val sourceClusterID = x._1(0)
        val temp = x._1.takeRight(x._1.length - 1).toList
        val tempPoint = new Point(x._2.toInt, temp)
        tempPoint.sourceClusterID = sourceClusterID.toInt
        tempPoint
      }.cache()

    val opticsSettings = new OpticsSettings(1.47, 6)
    val partitioningSettings = new PartitioningSettings(20)
    val spark_optics = new Spark_Optics1(opticsSettings, partitioningSettings)
    val opticsModel = spark_optics.run(dataSetArray)
    val dataArray = dataSetArray.collect()
    //    var result=opticsModel.getMap()
    val result = opticsModel.getResult()
    //    for(x<-dataArray)
    //    {
    //      if(!result.contains(x.pointID))
    //        result +=x.pointID->(x.sourceClusterID,opticsModel.predict(x))
    //    }

    var countZeroZero = 0.0
    var countZeroOne = 0.0
    var countOneOne = 0.0
    var countOneZero = 0.0
    var persicion = 0.0
    var countOne = 0.0
    var countZero = 0.0

    //    for(elem<-result)
    //    {
    //      if(elem._2._1==0&&elem._2._2== -1)
    //      {
    //        countZeroOne += 1
    //        countZero+=1
    //      }
    //      else if (elem._2._1==0&&elem._2._2!= -1)
    //      {
    //        countZeroZero+=1
    //        countZero+=1
    //      }
    //      else if (elem._2._1== 1&&elem._2._2== 0)
    //      {
    //        countOneZero += 1
    //        countOne+=1
    //      }
    //      else if (elem._2._1== 1 &&elem._2._2!=0)
    //      {
    //        countOneOne += 1
    //        countOne+=1
    //      }
    //    }

    for (elem <- result) {
      if (elem._2 == 0 && elem._3 == -1) {
        countZeroOne += 1
        countZero += 1
      }
      else if (elem._2 == 0 && elem._3 != -1) {
        countZeroZero += 1
        countZero += 1
      }
      else if (elem._2 == 1 && elem._3 == 0) {
        countOneZero += 1
        countOne += 1
      }
      else if (elem._2 == 1 && elem._3 != 0) {
        countOneOne += 1
        countOne += 1
      }
    }


    persicion = (countZeroZero / countZero + countOneOne / countOne) / 2.0

    val resultString = "RunningTime:" + opticsModel.runningTime + "\n" +
      "countZeroOne:" + countZeroOne + "\n" +
      "countZeroZero:" + countZeroZero + "\n" +
      "countOneZero:" + countOneZero + "\n" +
      "countOneOne:" + countOneOne + "\n" +
      "persicion:" + persicion

    sc.parallelize(resultString, 1).saveAsTextFile(args(2))
    sc.stop()
  }

}
