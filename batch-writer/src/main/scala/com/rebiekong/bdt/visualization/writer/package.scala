package com.rebiekong.bdt.visualization

import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

package object writer {
  implicit def toDataFrameFunctions(data: DataFrame): DataFrameFunctions = {
    new DataFrameFunctions(data)
  }
}
