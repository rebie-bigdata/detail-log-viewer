package com.rebiekong.bdt.visualization.writer

import org.apache.spark.sql.{DataFrame, SparkSession}

trait ReadTables {

  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read.table(sourceTable)
  }

  protected def sourceTable: String
}
