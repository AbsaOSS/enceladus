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

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructType}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.StandardizationPropertiesProvider
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.testUtils.TZNormalizedSparkTestBase
import za.co.absa.standardization.{RecordIdGeneration, Standardization}
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig}
import za.co.absa.standardization.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.testUtils.DataFrameTestUtils._

class NullValueStandardizationCsvSuite  extends AnyFunSuite with TZNormalizedSparkTestBase with MockitoSugar with DatasetComparer {
  private val argsBase = ("--dataset-name Foo --dataset-version 1 --report-date 2020-06-22 --report-version 1 " +
    "--rest-api-auth-keytab src/test/resources/user.keytab.example --raw-format csv --delimiter :")
    .split(" ")
  private implicit val dao: EnceladusDAO = mock[EnceladusDAO]
  private val metadataConfig = BasicMetadataColumnsConfig.fromDefault().copy(recordIdStrategy = RecordIdGeneration.IdType.NoId)
  private val config = BasicStandardizationConfig
    .fromDefault()
    .copy(metadataColumns = metadataConfig)

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

    val actualDF = Standardization.standardize(sourceDF, baseSchema, config)

    val expectedData = Seq(
      Row("a", "Arya", "Stark", Seq()),
      Row("b", "Jon", "Snow", Seq()),
      Row("c,Use,Comma", null, "", Seq(
        Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "last_name", Seq("null"), Seq())
      )),
      Row("d", null, "Hodor", Seq()),
      Row("e", "Pete", "Pete", Seq())
    )
    val expectedDF = expectedData.toDfWithSchema(actualDF.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
  }

  test("Reading data from CSV with custom delimiter, null-values set.") {
    val cmd = StandardizationConfig.getFromArguments(argsBase ++ Array("--null-value", "Pete"))

    val cvsReader = new StandardizationPropertiesProvider().getFormatSpecificReader(cmd, dataSet)

    val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema)
    val reader = cvsReader.schema(inputSchema)
    val sourceDF = reader.load("src/test/resources/data/standardization_csv_suite_data.csv")

    val actualDF = Standardization.standardize(sourceDF, baseSchema, config)

    val expectedData = Seq(
      Row("a", "Arya", "Stark", Seq()),
      Row("b", "Jon", "Snow", Seq()),
      Row("c,Use,Comma", null, "", Seq(
        Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "last_name", Seq("null"), Seq())
      )),
      Row("d", null, "Hodor", Seq()),
      Row("e", null, "", Seq(
        Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "last_name", Seq("null"), Seq()),
      ))
    )
    val expectedDF = expectedData.toDfWithSchema(actualDF.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
  }
}
