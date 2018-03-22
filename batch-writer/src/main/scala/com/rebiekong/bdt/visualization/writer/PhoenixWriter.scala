package com.rebiekong.bdt.visualization.writer

import org.apache.spark.sql.SparkSession

class PhoenixWriter(models: BasicTableModel*)(implicit spark: SparkSession) {

  def run(table: String, zkUrl: String): Unit = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    models.par.foreach(_.working(table, zkUrl))
  }


}
