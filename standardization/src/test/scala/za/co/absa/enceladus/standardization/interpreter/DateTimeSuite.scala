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

package za.co.absa.enceladus.standardization.interpreter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.standardization.interpreter.stages.SchemaChecker
import za.co.absa.enceladus.standardization.samples.TestSamples
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.validation.{SchemaValidator, ValidationError, ValidationException}

import scala.io.Source

class DateTimeSuite extends FunSuite with SparkTestBase {

  lazy val data: DataFrame = spark.createDataFrame(TestSamples.dateSamples)
  lazy val schemaWrong: StructType = DataType.fromJson(Source
    .fromFile("src/test/resources/dateTimestampSchemaWrong.json")
    .getLines().mkString("\n"))
    .asInstanceOf[StructType]
  lazy val schemaOk: StructType = DataType.fromJson(Source
    .fromFile("src/test/resources/dateTimestampSchemaOk.json")
    .getLines().mkString("\n"))
    .asInstanceOf[StructType]

  implicit val udfLib = UDFLibrary()

  test("Validation should return critical errors") {
    println(data.schema.prettyJson)
    val validationErrors = SchemaValidator.validateSchema(schemaWrong)
    val hasCriticalErrors = validationErrors.exists( p =>
      p.issues.exists {
        case issue: ValidationError => true
        case _ => false
      })
    assert(hasCriticalErrors)
  }

  test("Validation for this data should return critical errors") {
    val errors = SchemaChecker.validateSchemaAndLog(schemaWrong)
    assert(errors._1.nonEmpty)
  }

  test("Date Time Standardization Example Test should throw an exception") {
    val std = intercept[ValidationException] {
      StandardizationInterpreter.standardize(data, schemaWrong, "dates")
    }
  }

  test("Date Time Standardization Example with fixed schema should work") {
    val std = StandardizationInterpreter.standardize(data, schemaOk, "dates")
    std.show(false)
    std.printSchema
    println(std.schema.prettyJson)
  }
}
