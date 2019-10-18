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

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Outcome, fixture}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.fixtures.TempFileFixture
import za.co.absa.enceladus.standardization.{CmdConfig, StandardizationJob}
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.validation.ValidationException

class StandardizationRerunSuite extends fixture.FunSuite with SparkTestBase with TempFileFixture with MockitoSugar {

  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

  private implicit val udfLib: UDFLibrary = new UDFLibrary
  private implicit val dao: MenasDAO = mock[MenasDAO]

  private val tmpDirPrefix = "StdRerunTest"
  private val tmpFilePrefix = "test-input-"
  private val tmpFileSuffix = ".csv"

  private val csvContent: String =
    """101|102|1|2019-05-04|2019-05-04
      |201|202|2|2019-05-05|2019-05-05
      |301|302|1|2019-05-06|2019-05-06
      |401|402|1|2019-05-07|2019-05-07
      |501|502||2019-05-08|2019-05-08"""
      .stripMargin

  private val dataSet = Dataset("SpecialColumns", 1, None, "", "", "SpecialColumns", 1, conformance = Nil)

  type FixtureParam = String

  def withFixture(test: OneArgTest): Outcome = {
    val tmpFile = createTempFile(tmpFilePrefix, tmpFileSuffix, StandardCharsets.UTF_8, csvContent)
    val outcome = test(tmpFile.getAbsolutePath)
    tmpFile.delete()
    outcome
  }

  /** Creates a dataframe from an input file name path and command line arguments to Standardization */
  private def getTestDataFrame(tmpFileName: String, schemaWithStringType: StructType): DataFrame = {
    val args = ("--dataset-name SpecialColumns --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false --delimiter |").split(" ")

    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    StandardizationJob
      .getFormatSpecificReader(cmd, dataSet, schemaWithStringType.fields.length)
      .schema(schemaWithStringType)
      .load(tmpFileName)
  }

  test("Test standardizing a CSV without special columns") { tmpFileName =>
    val schema: StructType = StructType(Seq(
      StructField("A1", IntegerType, nullable = true),
      StructField("A2", IntegerType, nullable = true),
      StructField("A3", IntegerType, nullable = true),
      StructField("A4", StringType, nullable = true,
        Metadata.fromJson("""{"pattern": "yyyy-MM-dd"}""")),
      StructField("A5", StringType, nullable = true)
    ))

    val schemaWithStringType: StructType = StructType(Seq(
      StructField("A1", StringType, nullable = true),
      StructField("A2", StringType, nullable = true),
      StructField("A3", StringType, nullable = true),
      StructField("A4", StringType, nullable = true),
      StructField("A5", StringType, nullable = true)
    ))

    val expectedOutput =
      """+---+---+----+----------+----------+------+
        ||A1 |A2 |A3  |A4        |A5        |errCol|
        |+---+---+----+----------+----------+------+
        ||101|102|1   |2019-05-04|2019-05-04|[]    |
        ||201|202|2   |2019-05-05|2019-05-05|[]    |
        ||301|302|1   |2019-05-06|2019-05-06|[]    |
        ||401|402|1   |2019-05-07|2019-05-07|[]    |
        ||501|502|null|2019-05-08|2019-05-08|[]    |
        |+---+---+----+----------+----------+------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val inputDf = getTestDataFrame(tmpFileName, schemaWithStringType)

    val stdDf = StandardizationInterpreter.standardize(inputDf, schema, "").cache()

    val actualOutput = stdDf.dataAsString(truncate = false)

    assert(actualOutput == expectedOutput)
  }

  test("Test standardizing a CSV with special columns when error column has wrong type") { tmpFileName =>
    val schema: StructType = StructType(Seq(
      StructField("A1", IntegerType, nullable = true),
      StructField(ErrorMessage.errorColumnName, IntegerType, nullable = true),
      StructField("enceladus_info_version", IntegerType, nullable = true),
      StructField("enceladus_info_date", DateType, nullable = true,
        Metadata.fromJson("""{"pattern": "yyyy-MM-dd"}""")),
      StructField("enceladus_info_date_string", StringType, nullable = true)
    ))

    val schemaStr: StructType = StructType(Seq(
      StructField("A1", StringType, nullable = true),
      StructField(ErrorMessage.errorColumnName, StringType, nullable = true),
      StructField("enceladus_info_version", StringType, nullable = true),
      StructField("enceladus_info_date", StringType, nullable = true),
      StructField("enceladus_info_date_string", StringType, nullable = true)
    ))

    val inputDf = getTestDataFrame(tmpFileName, schemaStr)

    assertThrows[ValidationException] {
      StandardizationInterpreter.standardize(inputDf, schema, "").cache()
    }
  }

  test("Test standardizing a CSV with special columns when error column has correct type") { tmpFileName =>
    val schema: StructType = StructType(Seq(
      StructField("A1", IntegerType, nullable = true),
      StructField("A2", IntegerType, nullable = true),
      StructField("enceladus_info_version", IntegerType, nullable = false),
      StructField("enceladus_info_date", DateType, nullable = true,
        Metadata.fromJson("""{"pattern": "yyyy-MM-dd"}""")),
      StructField("enceladus_info_date_string", StringType, nullable = true)
    ))

    val schemaStr: StructType = StructType(Seq(
      StructField("A1", StringType, nullable = true),
      StructField("A2", StringType, nullable = true),
      StructField("enceladus_info_version", StringType, nullable = true),
      StructField("enceladus_info_date", StringType, nullable = true),
      StructField("enceladus_info_date_string", StringType, nullable = true)
    ))

    val inputDf = getTestDataFrame(tmpFileName, schemaStr)
      .withColumn(ErrorMessage.errorColumnName, typedLit(List[ErrorMessage]()))

    val stdDf = StandardizationInterpreter.standardize(inputDf, schema, "").cache()
    val failedRecords = stdDf.filter(size(col(ErrorMessage.errorColumnName)) > 0).count

    assert(stdDf.schema.exists(field => field.name == ErrorMessage.errorColumnName))
    assert(failedRecords == 1)
  }

}
