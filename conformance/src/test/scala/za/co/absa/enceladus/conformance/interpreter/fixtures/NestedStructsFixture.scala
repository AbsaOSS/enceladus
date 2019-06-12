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

  private val log: Logger = LogManager.getLogger(this.getClass)

  private val upperRule = UppercaseConformanceRule(order = 3, inputColumn = "MyLiteral", controlCheckpoint = false,
    outputColumn = "MyUpperLiteral")

  private val nestedStructsDS = Dataset(
    name = "Nested Structs Conformance",
    version = 1,
    hdfsPath = "src/test/testData/_nestedStructs",
    hdfsPublishPath = "src/test/testData/_conformedNestedStructs",
    schemaName = "NestedStructs", schemaVersion = 0,
    conformance = List(upperRule)
  )

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
