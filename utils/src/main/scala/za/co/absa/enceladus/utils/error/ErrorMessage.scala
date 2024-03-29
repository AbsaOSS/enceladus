/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.utils.error

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import za.co.absa.standardization.config.DefaultErrorCodesConfig

/**
 * Case class to represent an error message
 *
 * @param errType - Type or source of the error
 * @param errCode - Internal error code
 * @param errMsg - Textual description of the error
 * @param errCol - The name of the column where the error occurred
 * @param rawValues - Sequence of raw values (which are the potential culprits of the error)
 * @param mappings - Sequence of Mappings i.e Mapping Table Column -> Equivalent Mapped Dataset column
 */
case class ErrorMessage(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String], mappings: Seq[Mapping] = Seq())
case class Mapping(mappingTableColumn: String, mappedDatasetColumn: String)

object ErrorMessage {
  val errorColumnName = "errCol"

  def confMappingErr(errCol: String, rawValues: Seq[String], mappings: Seq[Mapping]): ErrorMessage = ErrorMessage(
    errType = "confMapError",
    errCode = ErrorCodes.ConfMapError,
    errMsg = "Conformance Error - Null produced by mapping conformance rule",
    errCol = errCol,
    rawValues = rawValues, mappings = mappings)
  def confCastErr(errCol: String, rawValue: String): ErrorMessage = ErrorMessage(
    errType = "confCastError",
    errCode = ErrorCodes.ConfCastErr,
    errMsg = "Conformance Error - Null returned by casting conformance rule",
    errCol = errCol,
    rawValues = Seq(rawValue))
  def confNegErr(errCol: String, rawValue: String): ErrorMessage = ErrorMessage(
    errType = "confNegError",
    errCode = ErrorCodes.ConfNegErr,
    errMsg = "Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged",
    errCol = errCol,
    rawValues = Seq(rawValue))
  def confLitErr(errCol: String, rawValue: String): ErrorMessage = ErrorMessage(
    errType = "confLitError",
    errCode = ErrorCodes.ConfLitErr,
    errMsg = "Conformance Error - Special column value has changed",
    errCol = errCol,
    rawValues = Seq(rawValue))

  def errorColSchema(implicit spark: SparkSession): StructType = {
    import spark.implicits._
    spark.emptyDataset[ErrorMessage].schema
  }

  /**
    * This object purpose it to group the error codes together to decrease a chance of them being in conflict
    */
  object ErrorCodes {
    final val ConfMapError    = "E00001"
    final val ConfCastErr     = "E00003"
    final val ConfNegErr      = "E00004"
    final val ConfLitErr      = "E00005"

    final val StdCastError    = DefaultErrorCodesConfig.castError
    final val StdNullError    = DefaultErrorCodesConfig.nullError
    final val StdTypeError    = DefaultErrorCodesConfig.typeError
    final val StdSchemaError  = DefaultErrorCodesConfig.schemaError

    val standardizationErrorCodes: Seq[String] = Seq(StdCastError, StdNullError, StdTypeError, StdSchemaError)
    val conformanceErrorCodes: Seq[String] = Seq(ConfMapError, ConfCastErr, ConfNegErr, ConfLitErr)
  }
}

