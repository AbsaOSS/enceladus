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

package za.co.absa.enceladus.conformance.interpreter.fixtures

import java.io.File

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import org.mockito.Mockito.{mock, when => mockWhen}

import scala.util.control.NonFatal

trait NestedStructsFixture extends BeforeAndAfterAll with SparkTestBase {

  this: Suite =>

  protected var standardizedDf: DataFrame = _

  implicit protected val dao: EnceladusDAO = mock(classOf[EnceladusDAO])
  implicit protected val progArgs: CmdConfig = CmdConfig(reportDate = "2017-11-01")

  protected val upperRule1 = UppercaseConformanceRule(order = 1, inputColumn = "strings.with_new_lines",
    controlCheckpoint = false, outputColumn = "strings.with_new_lines_upper")

  protected val upperRule2 = UppercaseConformanceRule(order = 2, inputColumn = "strings.all_random",
    controlCheckpoint = false, outputColumn = "strings.all_random_upper")

  protected val upperRule3 = UppercaseConformanceRule(order = 3, inputColumn = "strings.whitespaces",
    controlCheckpoint = false, outputColumn = "strings.whitespaces_upper")

  protected val negationRule1 = NegationConformanceRule(order = 4, inputColumn = "numerics.small_positive",
    controlCheckpoint = false, outputColumn = "numerics.small_positive_negated")

  protected val negationRule2 = NegationConformanceRule(order = 5, inputColumn = "numerics.small_negative",
    controlCheckpoint = false, outputColumn = "numerics.small_negative_negated")

  protected val negationRule3 = NegationConformanceRule(order = 6, inputColumn = "numerics.big_positive",
    controlCheckpoint = false, outputColumn = "numerics.big_positive_negated")

  protected val negationRule4 = NegationConformanceRule(order = 7, inputColumn = "numerics.big_negative",
    controlCheckpoint = false, outputColumn = "numerics.big_negative_negated")

  protected val nestedStructsDS = Dataset(
    name = "Nested Structs Conformance",
    version = 1,
    hdfsPath = "src/test/testData/_nestedStructs",
    hdfsPublishPath = "src/test/testData/_conformedNestedStructs",
    schemaName = "NestedStructs", schemaVersion = 0,
    conformance = List(upperRule1, upperRule2, upperRule3, negationRule1, negationRule2, negationRule3, negationRule4)
  )

  private val log: Logger = LogManager.getLogger(this.getClass)

  private val infoFileContents: String = IOUtils.toString(this.getClass
    .getResourceAsStream("/interpreter/nestedStructs/info.json"), "UTF-8")

  private val mappingTablePattern = "{0}/{1}/{2}"

  override protected def beforeAll(): Unit = {
    createDataFiles()
    prepareDataFrame()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally deleteDataFiles()
  }

  private def createDataFiles(): Unit = {
    val dfA = spark.read.json("src/test/testData/nestedStructs/data.json")
    dfA
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(getRawDataPath(nestedStructsDS.hdfsPath))
    FileUtils.writeStringToFile(new File(s"${getRawDataPath(nestedStructsDS.hdfsPath)}/_INFO"), infoFileContents)
  }

  private def prepareDataFrame(): Unit = {
    standardizedDf = DataSource.getData(nestedStructsDS.hdfsPath, "2017", "11", "01", mappingTablePattern)
  }

  private def prepareDao(): Unit = {
    mockWhen(dao.getDataset("Nested Structs Conformance", 1)) thenReturn nestedStructsDS
    mockWhen(dao.getSchema("Employee", 0)) thenReturn standardizedDf.schema
  }

  private def deleteDataFiles(): Unit = {
    safeDeleteDir(getRawDataPath(nestedStructsDS.hdfsPath))
  }

  private def safeDeleteDir(path: String): Unit = {
    try {
      FileUtils.deleteDirectory(new File(path))
    } catch {
      case NonFatal(e) => log.warn(s"Unable to delete a temporary directory $path")
    }
  }

  private def getRawDataPath(basePath: String): String = s"$basePath/2017/11/01/"

}
