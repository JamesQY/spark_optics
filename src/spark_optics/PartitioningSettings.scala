package spark_optics

/**
  * Created by Administrator on 2016/6/22.
  */
class PartitioningSettings(
                            val numberOfPointsInBlock: Long = PartitioningSettings.DefaultNumberOfPointsInBlock
                          ) extends Serializable {
}

object PartitioningSettings {

  def DefaultNumberOfPointsInBlock: Long = 50000

}
