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

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types._
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.testUtils.DataFrameTestUtils._
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.udf.UDFLibrary

class StandardizationXmlSuite extends AnyFunSuite with SparkTestBase with MockitoSugar with DatasetComparer {
  private implicit val udfLibrary:UDFLibrary = new UDFLibrary()

  private val standardizationReader = new StandardizationPropertiesProvider()

  test("Reading data from XML input") {

    implicit val dao: MenasDAO = mock[MenasDAO]

    val args = ("--dataset-name Foo --dataset-version 1 --report-date 2018-08-10 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format xml --row-tag instrument").split(" ")

    val dataSet = Dataset("SpecialChars", 1, None, "", "", "SpecialChars", 1, conformance = Nil)
    val cmd = StandardizationConfig.getFromArguments(args)

    val csvReader = standardizationReader.getFormatSpecificReader(cmd, dataSet)

    val baseSchema = StructType(Array(
      StructField("rowId", LongType),
      StructField("reportDate", StringType),
      StructField("legs", types.ArrayType(StructType(Array(
        StructField("leg", StructType(Array(
          StructField("price", IntegerType)
        )))
      ))))
    ))
    val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema, Option("_corrupt_record"))
    val reader = csvReader.schema(inputSchema)

    val sourceDF = reader.load("src/test/resources/data/standardization_xml_suite_data.txt")
    // not expecting corrupted records, but checking to be sure
    val corruptedRecords = sourceDF.filter(col("_corrupt_record").isNotNull)
    assert(corruptedRecords.isEmpty, s"Unexpected corrupted records found: ${corruptedRecords.collectAsList()}")

    val stdDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)

    val expectedData = Seq(
      Row(1L, "2018-08-10", Seq(Row(Row(1000))), Seq()),
      Row(2L, "2018-08-10", Seq(Row(Row(2000))), Seq()),
      Row(3L, "2018-08-10", Seq(Row(Row(null))), Seq()),
      Row(4L, "2018-08-10", null, Seq())
    )
    val expectedDF = expectedData.toDfWithSchema(stdDF.schema) // checking just the data
    assertSmallDatasetEquality(stdDF, expectedDF)
  }
}
