package za.co.absa.enceladus.conformance.interpreter.exceptions

import org.apache.spark.sql.types.DataType

case class InvalidDataTypeException(input: String, dataType: DataType) extends Exception(
  s"Data type ${dataType.typeName} is not valid or not supported"
)
