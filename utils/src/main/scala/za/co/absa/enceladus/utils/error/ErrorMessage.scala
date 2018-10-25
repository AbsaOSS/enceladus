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

  def stdCastErr(errCol: String, rawValue: String) = ErrorMessage(errType = "stdCastError", errCode = "E00000", errMsg = "Standardization Error - Type cast", errCol = errCol, rawValues = Seq(rawValue))
  def stdNullErr(errCol: String) = ErrorMessage(errType = "stdNullError", errCode = "E00002", errMsg = "Standardization Error - Null detected in non-nullable attribute", errCol = errCol, rawValues = Seq("null"))
  def confMappingErr(errCol: String, rawValues: Seq[String], mappings: Seq[Mapping]) =
    ErrorMessage(errType = "confMapError", errCode = "E00001", errMsg = "Conformance Error - Null produced by mapping conformance rule", errCol = errCol, rawValues = rawValues, mappings = mappings)
  def confCastErr(errCol: String, rawValue: String) = ErrorMessage(errType = "confCastError", errCode = "E00003", errMsg = "Conformance Error - Null returned by casting conformance rule", errCol = errCol, rawValues = Seq(rawValue))
  def confNegErr(errCol: String, rawValue: String) = ErrorMessage(errType = "confNegError", errCode = "E00004", errMsg = "Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged", errCol = errCol, rawValues = Seq(rawValue))

  def errorColSchema(implicit spark: SparkSession) = {
    import spark.implicits._
    spark.emptyDataset[ErrorMessage].schema
  }

}

