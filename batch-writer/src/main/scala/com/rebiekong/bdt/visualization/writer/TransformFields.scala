package com.rebiekong.bdt.visualization.writer

import java.sql.{Connection, DriverManager, JDBCType}

import org.apache.phoenix.schema.types.PDataType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

trait TransformFields {

  import TransformFields._

  def transform(input: Dataset[Row]): Dataset[Row] = {
    val fields = ListBuffer.empty[Column]
    fields.append(ID_FIELD_VALUE as ID_FIELD_NAME)
    fields.append(toTimeColumn(timeFieldValue, TIME_FIELD_NAME, timeFieldType))
    fields.append(lit(ID_TYPE_VALUE) as ID_TYPE_NAME)
    fields.append(lit(LOG_TYPE) as LOG_TYPE_NAME)

    val dataField = input.schema.map(_.name).filterNot(_.equals(ID_FIELD_VALUE))
    if (canSign) {
      fields.append(md5(concat_ws("|", dataField.map(toColumn)
        .map(_ cast DataTypes.StringType): _*)) as TransformFields.SIGN_FIELD_NAME)
    } else {
      fields.append(lit("no_sign") as TransformFields.SIGN_FIELD_NAME)
    }
    fields.appendAll(dataField.map(toColumn))

    input.select(fields: _*)
      .where(trim(new Column(ID_FIELD_NAME)).notEqual(""))
      .where(new Column(ID_FIELD_NAME).isNotNull)
      .where(new Column(TIME_FIELD_NAME).isNotNull)
      .where(new Column(SIGN_FIELD_NAME).isNotNull)
  }

  protected def canSign: Boolean = true

  protected def toTimeColumn(field: String, resultField: String, sqlType: JDBCType): Column = {
    sqlType match {
      case java.sql.JDBCType.INTEGER => from_unixtime(field, "yyyy-MM-dd HH:mm:ss") as resultField
      case _ => throw new Exception("NOT_SUPPORT")
    }
  }

  protected def ID_FIELD_VALUE: String

  protected def ID_TYPE_VALUE: String

  protected def LOG_TYPE: String

  protected def timeFieldValue: String

  protected def timeFieldType: JDBCType

  protected implicit def toColumn(field: String): Column = new Column(field)

}

object TransformFields {

  protected val ID_FIELD_NAME = "id"
  protected val ID_TYPE_NAME = "id_type"
  protected val LOG_TYPE_NAME = "log_type"
  protected val SIGN_FIELD_NAME = "sign"
  protected val TIME_FIELD_NAME = "server_date"

  protected val pk: List[String] = List[String](
    ID_FIELD_NAME, TIME_FIELD_NAME, SIGN_FIELD_NAME
  )

  private[writer] def alter(df: Dataset[Row], table: String, phoenixURL: String) {
    val connection = DriverManager.getConnection(phoenixURL)
    alter(df, table, connection)
    connection.close()
  }

  protected def alter(df: Dataset[Row], table: String, connection: Connection) {
    val stm = connection.createStatement()
    df.schema.filterNot(f => pk.contains(f.name)).foreach(
      structField => {
        val alter = "ALTER TABLE " + table + " ADD IF NOT EXISTS " +
          structField.name + " " + PDataType.fromTypeId(fixToPhoenix(structField.dataType)).toString
        println(alter)
        stm.execute(alter)
      }
    )
    stm.close()
    connection.commit()
  }

  protected def fixToPhoenix(dt: DataType): Int = {
    dt match {
      case IntegerType => java.sql.Types.INTEGER
      case LongType => java.sql.Types.BIGINT
      case DoubleType => java.sql.Types.DOUBLE
      case FloatType => java.sql.Types.FLOAT
      case ShortType => java.sql.Types.SMALLINT
      case ByteType => java.sql.Types.TINYINT
      case BooleanType => java.sql.Types.BOOLEAN
      case StringType => java.sql.Types.VARCHAR
      case BinaryType => java.sql.Types.BINARY
      case TimestampType => java.sql.Types.TIMESTAMP
      case DateType => java.sql.Types.DATE
      case _: DecimalType => java.sql.Types.DECIMAL
      case _ => java.sql.Types.VARCHAR
    }
  }
}
