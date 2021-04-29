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

package za.co.absa.enceladus.conformance.interpreter.fixtures

import java.io.File
import java.nio.charset.Charset

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.mockito.Mockito.{when => mockWhen}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.mockito.Mockito.mock
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.{Dataset, MappingTable}
import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

import scala.io.Source.fromFile
import scala.util.control.NonFatal

trait MultipleMappingFixture extends BeforeAndAfterAll with SparkTestBase {

  this: Suite =>

  protected var standardizedDf: DataFrame = _

  implicit protected val dao: MenasDAO = mock(classOf[MenasDAO])
  implicit protected val progArgs: ConformanceConfig = ConformanceConfig(reportDate = "2020-03-23")

  protected val mappingConformanceLocationRuleSimple: MappingConformanceRule = MappingConformanceRule(order = 1,
    controlCheckpoint = false, mappingTable = "locationMt", mappingTableVersion = 1,
    attributeMappings = Map("location" -> "location") ,targetAttribute = "show", outputColumn = "conf_show",
    additionalColumns = Some(Map()))
  protected val mappingConformanceRuleTownsNested: MappingConformanceRule = MappingConformanceRule(order = 1,
    controlCheckpoint = false, mappingTable = "townsMt", mappingTableVersion = 1,
    attributeMappings = Map("town" -> "address.town"), targetAttribute = "shortcut", outputColumn = "address.town_shortcut",
    additionalColumns = Some(Map("address.zip_code"->"zip_code")))
  protected val mappingConformanceRuleAccessesArray: MappingConformanceRule = MappingConformanceRule(order = 1,
    controlCheckpoint = false, mappingTable = "roomsMt", mappingTableVersion = 1,
    attributeMappings = Map("place" -> "accesses.place") ,targetAttribute = "opening_hours", outputColumn = "accesses.new_opening_hours",
    additionalColumns = Some(Map("accesses.conformed_place"->"place")))

  protected val mappingDS: Dataset = Dataset(
    name = "Mapping multiple types Conformance",
    hdfsPath = "src/test/testData/_mappingData",
    hdfsPublishPath = "src/test/testData/_conformedMappingData",
    schemaName = "MappingSchema", schemaVersion = 0,
    conformance = List(mappingConformanceLocationRuleSimple, mappingConformanceRuleTownsNested, mappingConformanceRuleAccessesArray)
  )

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  override protected def beforeAll(): Unit = {
    createDataFiles()
    prepareDataFrame()
    prepareDao()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally deleteDataFiles()
  }

  private def createDataFiles(): Unit = {
    val dfA = spark.read.json("../examples/data/e2e_tests/data/hdfs/std/mappingConformanceRuleData/2020/03/23/v1/data.json")
    dfA
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(getRawDataPath(mappingDS.hdfsPath))
    val value = "../examples/data/e2e_tests/data/hdfs/std/mappingConformanceRuleData/2020/03/23/v1/_INFO"
    val infoFileContents: String = fromFile(value, "UTF-8").mkString

    FileUtils.writeStringToFile(new File(s"${getRawDataPath(mappingDS.hdfsPath)}/_INFO"), infoFileContents,
      Charset.defaultCharset, true)
  }

  private def prepareDataFrame(): Unit = {
    standardizedDf = DataSource.getDataFrame(mappingDS.hdfsPath, "2017-11-01", mappingTablePattern)
  }

  private val mappingTablePattern = "{0}/{1}/{2}"

  private def deleteDataFiles(): Unit = {
    safeDeleteDir(getRawDataPath(mappingDS.hdfsPath))
  }

  val locationMT: MappingTable = MappingTable(name = "locationMt", hdfsPath = "../examples/data/e2e_tests/data/hdfs/std/map/location",
    schemaName = "location", schemaVersion = 1)

  val townsMT: MappingTable = MappingTable(name = "townsMt", hdfsPath = "../examples/data/e2e_tests/data/hdfs/std/map/towns",
    schemaName = "towns", schemaVersion = 1)

  val roomsMT: MappingTable = MappingTable(name = "roomsMt", hdfsPath = "../examples/data/e2e_tests/data/hdfs/std/map/rooms",
    schemaName = "rooms", schemaVersion = 1)

  private def prepareDao(): Unit = {
    mockWhen(dao.getSchema("location", 1)) thenReturn StructType(Seq(StructField("location",StringType),
      StructField("show",StringType)))
    mockWhen(dao.getMappingTable("locationMt",1)) thenReturn locationMT

    mockWhen(dao.getSchema("towns", 1)) thenReturn StructType(Seq(StructField("town", StringType),
      StructField("shortcut", StringType), StructField("zip_code", StringType)))
    mockWhen(dao.getMappingTable("townsMt",1)) thenReturn townsMT

    mockWhen(dao.getSchema("rooms", 1)) thenReturn StructType(Seq(StructField("place",StringType),
      StructField("opening_hours",StringType)))
    mockWhen(dao.getMappingTable("roomsMt",1)) thenReturn roomsMT
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
