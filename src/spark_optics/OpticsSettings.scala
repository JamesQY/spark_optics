package spark_optics

/**
  * Created by Administrator on 2016/6/22.
  */
class OpticsSettings(val epsilon: Double = OpticsSettings.getDefaultEpsilon,
                     val numPoints: Int = OpticsSettings.getDefaultNumberOfPoints) extends Serializable {

}

object OpticsSettings {
  def getDefaultEpsilon: Double = {
    2
  }

  def getDefaultNumberOfPoints: Int = {
    4
  }
}