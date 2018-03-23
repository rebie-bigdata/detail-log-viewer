package models

import com.rebiekong.bdt.visualization.writer.BasicTableModel
import org.apache.spark.sql.SparkSession

class InstallModel()(implicit spark: SparkSession) extends BasicTableModel {

  override protected def ID_FIELD_VALUE = "mid"

  override protected def ID_TYPE_VALUE = "mid"

  override protected def LOG_TYPE = "install"

  override protected def timeFieldValue = "server_time"

  override protected def timeFieldType = java.sql.JDBCType.INTEGER

  override protected def sourceTable = "cn_aunbox_ods.fact_install_pc_log"
}
