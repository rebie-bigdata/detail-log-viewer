package demo

import com.rebiekong.bdt.visualization.writer.BasicTableModel
import org.apache.spark.sql.SparkSession

class DemoModel()(implicit spark: SparkSession) extends BasicTableModel {

  override protected def ID_FIELD_VALUE = "uid"

  override protected def ID_TYPE_VALUE = "uid"

  override protected def LOG_TYPE = "user_log"

  override protected def timeFieldValue = "server_time"

  override protected def timeFieldType = java.sql.JDBCType.TIMESTAMP

  override protected def sourceTable = "cn_rebiekong_logs.user_visit_log"
}
