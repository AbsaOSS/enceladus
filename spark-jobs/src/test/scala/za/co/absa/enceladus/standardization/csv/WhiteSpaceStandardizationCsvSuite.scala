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

package za.co.absa.enceladus.standardization.csv

import org.apache.spark.sql.types.{DataType, StructType}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.StandardizationPropertiesProvider
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.udf.UDFLibrary

case class Person(id: String, first_name: String, last_name: String)

class WhiteSpaceStandardizationCsvSuite  extends AnyFunSuite with SparkTestBase with MockitoSugar {
  private implicit val udfLibrary: UDFLibrary = new UDFLibrary()
  private val argsBase = ("--dataset-name Foo --dataset-version 1 --report-date 2020-06-22 --report-version 1 " +
    "--menas-auth-keytab src/test/resources/user.keytab.example --raw-format csv --delimiter :")
    .split(" ")
  private implicit val dao: MenasDAO = mock[MenasDAO]

  private val dataSet = Dataset("Foo", 1, None, "", "", "SpecialChars", 1, conformance = Nil)

  private val baseSchema: StructType = DataType.fromJson(
    FileReader.readFileAsString(getClass.getResource("/data/standardization_csv_suite_schema.json").getPath)
  ).asInstanceOf[StructType]

  private val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema)

  import spark.implicits._

  test("Reading data from CSV with custom delimiter, no trimming set.") {
    val cmd = StandardizationConfig.getFromArguments(argsBase)
    val cvsReader = new StandardizationPropertiesProvider().getFormatSpecificReader(cmd, dataSet)
    val reader = cvsReader.schema(inputSchema)
    val sourceDF = reader.load(getClass.getResource("/data/standardization_csv_suite_data_with_white_spaces.csv").getPath)

    val testDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)
    val actual = testDF.select("id", "first_name", "last_name").as[Person].collect()
    val expected = Array(
      Person("a","Arya  ","  Stark"),
      Person("b","Jon   ","  Snow"),
      Person("d","      ","  Hodor"),
      Person("e","Pete  ","  Pete")
    )
    assert(expected sameElements actual)
  }

  test("Reading data from CSV with custom delimiter, ignore leading white spaces.") {
    val cmd = StandardizationConfig.getFromArguments(argsBase ++ Array("--csv-ignore-leading-white-space", "true"))
    val cvsReader = new StandardizationPropertiesProvider().getFormatSpecificReader(cmd, dataSet)
    val reader = cvsReader.schema(inputSchema)
    val sourceDF = reader.load(getClass.getResource("/data/standardization_csv_suite_data_with_white_spaces.csv").getPath)

    val testDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)
    val actual = testDF.select("id", "first_name", "last_name").as[Person].collect()
    val expected = Array(
      Person("a", "Arya  ", "Stark"),
      Person("b", "Jon   ", "Snow"),
      Person("d", null, "Hodor"),
      Person("e", "Pete  ", "Pete")
    )

    assert(expected sameElements actual)
  }

  test("Reading data from CSV with custom delimiter, null-values set.") {
    val cmd = StandardizationConfig.getFromArguments(argsBase ++ Array("--csv-ignore-trailing-white-space", "true"))
    val cvsReader = new StandardizationPropertiesProvider().getFormatSpecificReader(cmd, dataSet)
    val reader = cvsReader.schema(inputSchema)
    val sourceDF = reader.load(getClass.getResource("/data/standardization_csv_suite_data_with_white_spaces.csv").getPath)

    val testDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)
    val actual = testDF.select("id", "first_name", "last_name").as[Person].collect()
    val expected = Array(
      Person("a", "Arya", "  Stark"),
      Person("b", "Jon", "  Snow"),
      Person("d", null, "  Hodor"),
      Person("e", "Pete", "  Pete")
    )

    assert(expected sameElements actual)
  }
}
