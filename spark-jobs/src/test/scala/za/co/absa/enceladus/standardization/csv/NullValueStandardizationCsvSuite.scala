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
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults}
import za.co.absa.enceladus.utils.udf.UDFLibrary

class NullValueStandardizationCsvSuite  extends AnyFunSuite with SparkTestBase with MockitoSugar {
  private implicit val udfLibrary: UDFLibrary = new UDFLibrary()
  private val argsBase = ("--dataset-name Foo --dataset-version 1 --report-date 2020-06-22 --report-version 1 " +
    "--menas-auth-keytab src/test/resources/user.keytab.example --raw-format csv --delimiter :")
    .split(" ")
  private implicit val dao: MenasDAO = mock[MenasDAO]
  private implicit val defaults: Defaults = GlobalDefaults

  private val dataSet = Dataset("Foo", 1, None, "", "", "SpecialChars", 1, conformance = Nil)

  private val baseSchema: StructType = DataType.fromJson(
    FileReader.readFileAsString("src/test/resources/data/standardization_csv_suite_schema.json")
  ).asInstanceOf[StructType]

  test("Reading data from CSV with custom delimiter, no null-values set.") {
    val cmd = StandardizationConfig.getFromArguments(argsBase)

    val cvsReader = new StandardizationPropertiesProvider().getFormatSpecificReader(cmd, dataSet)

    val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema)
    val reader = cvsReader.schema(inputSchema)
    val sourceDF = reader.load("src/test/resources/data/standardization_csv_suite_data.csv")

    val expected = FileReader.readFileAsString("src/test/resources/data/standardization_csv_suite_expected_no_null_value.txt")
      .replace("\r\n", "\n")

    val destDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(expected == actual)
  }

  test("Reading data from CSV with custom delimiter, null-values set.") {
    val cmd = StandardizationConfig.getFromArguments(argsBase ++ Array("--null-value", "Pete"))

    val cvsReader = new StandardizationPropertiesProvider().getFormatSpecificReader(cmd, dataSet)

    val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema)
    val reader = cvsReader.schema(inputSchema)
    val sourceDF = reader.load("src/test/resources/data/standardization_csv_suite_data.csv")

    val expected = FileReader.readFileAsString("src/test/resources/data/standardization_csv_suite_expected_with_null_value.txt")
      .replace("\r\n", "\n")

    val destDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(expected == actual)
  }
}
