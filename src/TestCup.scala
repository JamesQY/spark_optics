import org.apache.spark.{SparkConf, SparkContext}
import spark_optics._

/**
  * Created by root on 7/8/16.
  */
object TestCup {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testSpark_Optics")
      .set("spark.akka.frameSize", "2047").set("spark.driver.maxResultSize", "4096m")
      .set("spark.default.parallelism", "100").set("spark.shuffle.spill", "true").set("spark.shuffle.memoryFraction", "0.3")
    val sc = new SparkContext(conf)

    val dataSetArray = sc.textFile(args(0), 100).map(x => x.split(",", -1).map(_.toDouble)).zipWithIndex()
      .map { x =>
        val sourceClusterID = x._1(x._1.length - 1)
        val temp = x._1.take(x._1.length - 2).toList
        val tempPoint = new Point(x._2.toInt, temp)
        tempPoint.sourceClusterID = sourceClusterID.toInt
        tempPoint
      }.cache()

    val opticsSettings = new OpticsSettings(0.3, 6)
    val partitioningSettings = new PartitioningSettings(args(1).toInt)
    val spark_optics = new Spark_Optics1(opticsSettings, partitioningSettings)
    val opticsModel = spark_optics.run(dataSetArray)

    val runTime = Array(opticsModel.runningTime)

    val result = sc.parallelize(opticsModel.getResult(), 1)
    val runingTime = sc.parallelize(runTime, 1)
    result.saveAsTextFile(args(2))
    runingTime.saveAsTextFile(args(3))
    sc.stop()

  }
}
