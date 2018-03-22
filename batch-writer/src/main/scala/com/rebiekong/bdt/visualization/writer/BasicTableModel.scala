package com.rebiekong.bdt.visualization.writer

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BasicTableModel()(implicit spark: SparkSession) extends TransformFields with ReadTables {

  def working(table: String, zkUrl: String): Unit = {

    read().writeToPhoenixTable(table, zkUrl, fixFormat = true)
  }

  override def read()(implicit spark: SparkSession): DataFrame = {
    super.read.transform(transform).repartition(13)
  }

}

