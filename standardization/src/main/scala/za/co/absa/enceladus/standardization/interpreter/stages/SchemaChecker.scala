/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.enceladus.standardization.interpreter.stages

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import za.co.absa.enceladus.utils.validation.SchemaValidator.{validateErrorColumn, validateSchema}
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue, ValidationWarning}

object SchemaChecker {

  val log: Logger = LogManager.getLogger(this.getClass)

  /**
    * Validate a schema, log all errors and warnings, throws if there are fatal errors
    *
    * @param schema A Spark schema
    */
  def validateSchemaAndLog(schema: StructType)
                          (implicit spark: SparkSession): (Seq[String], Seq[String]) = {
    val failures = validateSchema(schema) ::: validateErrorColumn(schema)

    type ColName = String
    type Pattern = String

    val flattenedIssues: Seq[(ColName, Pattern, ValidationIssue)] =
      for {
        failure <- failures
        issue <- failure.issues
      } yield (failure.fieldName, failure.pattern, issue)

    // This code crafts 2 lists of messages. The first one will contain all errors and the second will contain all warnings.
    //
    // This code was a result of a long discussion. It's still not perfect, but it balances
    // trade-offs between readability, conciseness and performance.
    //
    val errorMessages: (Seq[String], Seq[String]) = flattenedIssues.foldLeft(Nil: Seq[String], Nil: Seq[String])((accumulator, current) => {
      val (errors, warnings) = accumulator
      val (column, pattern, issue) = current
      issue match {
        case ValidationError(text) =>
          val msg = s"Validation error for column '$column', pattern '$pattern': $text"
          log.error(msg)
          (errors :+ msg, warnings)
        case ValidationWarning(text) =>
          val msg = s"Validation warning for column '$column', pattern '$pattern': $text"
          log.warn(msg)
          (errors, warnings :+ msg)
      }
    })

    // Left out this code as well as it has it's own beauty and it's more efficient

    // val errorMessages: (Seq[String], Seq[String]) = ( (Nil: Seq[String], Nil: Seq[String]) /: flattenedIssues ) {
    //   case ((fieldNames, patterns), (column, pattern, ValidationError(text))) =>
    //     val msg = s"Validation error for column '$column', pattern '$pattern': $text"
    //     log.error(msg)
    //     (fieldNames :+ msg, patterns)
    //   case ((fieldNames, patterns), (column, pattern, ValidationWarning(text))) =>
    //     val msg = s"Validation warning for column '$column', pattern '$pattern': $text"
    //     log.warn(msg)
    //     (fieldNames, patterns :+ msg)
    // }

    errorMessages
  }

}
