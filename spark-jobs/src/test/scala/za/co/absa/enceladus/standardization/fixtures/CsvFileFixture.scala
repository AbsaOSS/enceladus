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

package za.co.absa.enceladus.standardization.fixtures

import java.io.File
import java.nio.charset.{Charset, StandardCharsets}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.mockito.MockitoSugar
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.{StandardizationJob, StdCmdConfig}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

trait CsvFileFixture extends MockitoSugar with TempFileFixture with SparkTestBase {
  private implicit val dao: MenasDAO = mock[MenasDAO]

  type FixtureParam = String
  private val tmpFilePrefix = "special-characters"
  protected val tmpFileSuffix = ".csv"
  protected val csvCharset: Charset = StandardCharsets.ISO_8859_1

  protected val schemaSeq = Seq(
    StructField("A1", StringType, nullable = true),
    StructField("A2", StringType, nullable = true),
    StructField("A3", StringType, nullable = true),
    StructField("A4", IntegerType, nullable = true),
    StructField("A5", IntegerType, nullable = true))

  protected val schemaWithoutCorruptRecord: StructType = StructType(schemaSeq)
  protected val schemaWithCorruptRecord: StructType = StructType(schemaSeq ++ Seq(StructField("_corrupt_record",
    StringType, nullable = true)))

  protected val dataSet: Dataset = Dataset("SpecialChars", 1, None, "", "", "SpecialChars", 1, conformance = Nil)

  /** Creates a dataframe from an input file name path and command line arguments to Standardization */
  def getTestCsvDataFrame(tmpFileName: String,
                          args: Array[String],
                          checkMaxColumns: Boolean = false,
                          dataSet: Dataset,
                          schema: StructType
                         ): DataFrame = {
    val cmd: StdCmdConfig = StdCmdConfig.getCmdLineArguments(args)
    val csvReader = if (checkMaxColumns) {
      StandardizationJob.getFormatSpecificReader(cmd, dataSet, schema.fields.length)
    } else {
      StandardizationJob.getFormatSpecificReader(cmd, dataSet)
    }
    csvReader
      .schema(schema)
      .load(tmpFileName)
  }

  /**
   * Creates a temporary csv file according to the fixed schema and returns the full path to it
   *
   * @param content      Contents to put to the file
   * @return The full path to the temporary csv file
   */
  def createTempCsvFile(content: String): File = {
    createTempFile(tmpFilePrefix, tmpFileSuffix, csvCharset, content)
  }
}
