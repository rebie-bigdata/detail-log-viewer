import com.rebiekong.bdt.visualization.writer.{BasicTableModel, PhoenixWriter}
import models.{InstallModel, SoftwareLogModel}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Apps extends App {

  System.setProperty("HADOOP_USER_NAME", "hadoop")
  implicit val spark: SparkSession = SparkSession.builder()
    .enableHiveSupport()
    .config("spark.sql.orc.impl", "native")
    .getOrCreate()

  val models = ListBuffer.empty[BasicTableModel]

  models.append(new SoftwareLogModel())
  models.append(new InstallModel())

  val writer = new PhoenixWriter(models: _*)
  writer.run("CN_AUNBOX_ODS.FULL_LOG", "192.168.7.5:2181")
}
