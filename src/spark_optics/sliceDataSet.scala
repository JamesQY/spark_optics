package spark_optics

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/7/6.
  */
object sliceDataSet {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("testSpark_Optics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val dataSetArray = sc.textFile("./src/test.txt").map(x => x.split(",").toList.tail.map(x => x.toDouble)).collect()
    val write = new PrintWriter("./src/test1.csv")
    for (elem <- dataSetArray)
      write.println(elem.mkString("", ",", "").replaceAll("\\(|\\)", "").replaceAll("ArrayBuffer", ""))
    write.close()
  }
}
