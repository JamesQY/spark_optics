package spark_optics

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/7/8.
  */
object dealData {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testSpark_Optics").setMaster("local[*]").set("spark.driver.maxResultSize", "2048m")
    val sc = new SparkContext(conf)
    val data = sc.textFile("./src/kddcup.csv", 8).map(x => x.split(",", -1)).cache()

    val data2 = data.map(x => x(2)).distinct().collect()
    println("data2.length:" + data2.length)
    val data1 = data.map(x => x(1)).distinct().collect()
    println("data1.length:" + data1.length)
    val data3 = data.map(x => x(3)).distinct().collect()
    println("data3.length:" + data3.length)
    val data41 = data.map(x => x(41)).distinct().collect()
    println("data41.length:" + data41.length)

    val dataTo2 = data.map { x =>
      for (i <- 0 until data2.length)
        if (data2(i) == x(2))
          x(2) = i.toString

      x
    }.cache()
    val dataTo1 = dataTo2.map { x =>
      for (i <- 0 until data1.length)
        if (data1(i) == x(1))
          x(1) = i.toString
      x
    }
    val dataTo3 = dataTo1.map { x =>
      for (i <- 0 until data3.length)
        if (data3(i) == x(3))
          x(3) = i.toString
      x
    }
    val dataTo41 = dataTo3.map { x =>
      for (i <- 0 until data41.length)
        if (data41(i) == x(41))
          x(41) = i.toString
      x
    }.take(10).foreach(x => println(x.mkString("", ",", "")))

    //    println(dataTo41.first().length)
    //      .collect()


    //    val write=new PrintWriter("./src/kddCup.csv")
    //    for(x<-dataTo41)
    //      {
    //        write.println(x.mkString("",",",""))
    //      }
    //    write.close()
  }
}
