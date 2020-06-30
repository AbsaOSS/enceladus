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

package za.co.absa.enceladus.standardization

import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import org.slf4j.Logger
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.enceladus.utils.udf.UDFLibrary

class StandardizationJsonSuite extends FunSuite with SparkTestBase with MockitoSugar{
  private implicit val udfLibrary:UDFLibrary = new UDFLibrary()

  private val standardizationReader = new PropertiesProvider()

  test("Reading data from JSON input, also such that don't adhere to desired schema") {

    implicit val dao: MenasDAO = mock[MenasDAO]

    val args = ("--dataset-name Foo --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format json").split(" ")

    val dataSet = Dataset("SpecialChars", 1, None, "", "", "SpecialChars", 1, conformance = Nil)
    val cmd = StandardizationConfig.getFromArguments(args)

    val csvReader = standardizationReader.getFormatSpecificReader(cmd, dataSet)

    val baseSchema: StructType = DataType.fromJson(
      FileReader.readFileAsString("src/test/resources/data/standardization_json_suite_schema.json")
    ).asInstanceOf[StructType]
    val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema, Option("_corrupt_record"))
    val reader = csvReader.schema(inputSchema)

    val sourceDF = reader.load("src/test/resources/data/standardization_json_suite_data.json")

    val expected = FileReader.readFileAsString("src/test/resources/data/standardization_json_suite_expected.txt")
      .replace("\r\n", "\n")

    val destDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }
}
