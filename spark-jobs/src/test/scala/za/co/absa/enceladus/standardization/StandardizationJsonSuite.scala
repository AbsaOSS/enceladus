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

import java.sql.{Date, Timestamp}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructType}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.testUtils.DataFrameTestUtils._
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults}
import za.co.absa.enceladus.utils.udf.UDFLibrary

class StandardizationJsonSuite extends AnyFunSuite with SparkTestBase with MockitoSugar with DatasetComparer{
  private implicit val udfLibrary:UDFLibrary = new UDFLibrary()
  private implicit val defaults: Defaults = GlobalDefaults

  private val standardizationReader = new StandardizationPropertiesProvider()

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
    val actualDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)

    val expectedData = Seq(
      Row("Having data", "Hello world", true, 1, Date.valueOf("2005-07-31"), Seq(Timestamp.valueOf("2005-07-31 08:22:31"), Timestamp.valueOf("2005-07-31 18:22:44")), Seq()),
      Row("Typed data", "Lorem Ipsum", true, 1000, Date.valueOf("2005-08-07"), Seq(), Seq()),
      Row("Nulls", null, null, null, null, null, Seq()),
      Row("Missing fields", null, null, null, null, null, Seq()),
      Row("Object in atomic type", null, null, null, null, null, Seq(
        Row("stdSchemaError", "E00007", "The input data does not adhere to requested schema", null, Seq("""{"description":"Object in atomic type","string":{"foo":bar}},"""), Seq())
      )),
      Row("Array in atomic type", null, null, null, null, null, Seq(
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "boolean", Seq("[false,true]"), Seq())
      )),
      Row("Array in string type", """["a","bb","ccc"]""", null, null, null, null, Seq()),
      Row("ERROR", null, null, null, null, null, Seq(
        Row("stdSchemaError", "E00007", "The input data does not adhere to requested schema", null, Seq("""{"description":"Object in array type","array":{"foo":bar}},"""), Seq()),
        Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "description", Seq("null"), Seq())
      )),
      Row("Atomic type in array", null, null, null, null, null, Seq(
        Row("stdSchemaError", "E00007", "The input data does not adhere to requested schema", null, Seq("""{"description":"Atomic type in array","array":"25/09/2005 11:22:33"},"""), Seq())
      ))
    )
    val expectedDF = expectedData.toDfWithSchema(actualDF.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
  }
}
