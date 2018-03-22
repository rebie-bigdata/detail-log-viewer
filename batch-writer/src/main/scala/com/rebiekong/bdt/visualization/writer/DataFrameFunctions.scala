package com.rebiekong.bdt.visualization.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

class DataFrameFunctions(data: DataFrame) {

  def writeToPhoenixTable(table: String, zkUrl: String, fixFormat: Boolean = false): Unit = {
    if (fixFormat) {
      data.alterStruct("jdbc:phoenix:" + zkUrl, table)
      Thread.sleep(10000L)
    }
    data
      .write.format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("table", table)
      .option("zkUrl", zkUrl)
      .save()

  }

  def alterStruct(phoenixUrl: String, dstTable: String): Unit = {
    TransformFields.alter(data, dstTable, phoenixUrl)
  }
}
