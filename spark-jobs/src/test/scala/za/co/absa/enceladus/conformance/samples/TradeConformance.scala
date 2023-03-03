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

package za.co.absa.enceladus.conformance.samples

import java.io.File
import java.nio.charset.Charset

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.model.{Dataset, DefaultValue, MappingTable}

import scala.util.control.NonFatal

object TradeConformance {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  val countryMT = MappingTable(name = "country", version = 0, hdfsPath = "src/test/testData/_countryMT",
    schemaName = "country", schemaVersion = 0)

  val currencyMT = MappingTable(name = "currency", version = 0, hdfsPath = "src/test/testData/_currencyMT",
    schemaName = "currency", schemaVersion = 0, defaultMappingValue = List(DefaultValue("currency_name", "\"Unknown\"")))

  val productMT = MappingTable(name = "product", version = 0, hdfsPath = "src/test/testData/_productMT",
    schemaName = "product", schemaVersion = 0, defaultMappingValue =
      List(DefaultValue("product_name", "\"Unknown1\""), DefaultValue("*", "\"Unknown2\"")))

  val countryRule = MappingConformanceRule(order = 1, mappingTable = "country", controlCheckpoint = true,
    mappingTableVersion = 0, attributeMappings = Map("country_code" -> "legs.conditions.country"),
    targetAttribute = "country_name", outputColumn = "legs.conditions.conformed_country")

  val litRule = LiteralConformanceRule(order = 2, outputColumn = "MyLiteral", controlCheckpoint = true, value = "abcdef")

  val upperRule = UppercaseConformanceRule(order = 3, inputColumn = "MyLiteral", controlCheckpoint = false,
    outputColumn = "MyUpperLiteral")

  val currencyRule = MappingConformanceRule(order = 4, mappingTable = "currency", controlCheckpoint = true,
    mappingTableVersion = 0, attributeMappings = Map("currency_code" -> "legs.conditions.currency"),
    targetAttribute = "currency_name", outputColumn = "legs.conditions.conformed_currency")

  val productRule = MappingConformanceRule(order = 5, mappingTable = "product", controlCheckpoint = true,
    mappingTableVersion = 0, attributeMappings = Map("product_code" -> "legs.conditions.product"),
    targetAttribute = "product_name", outputColumn = "legs.conditions.conformed_product")

  val product2Rule = MappingConformanceRule(order = 6, mappingTable = "product", controlCheckpoint = true,
    mappingTableVersion = 0, attributeMappings = Map("product_code" -> "legs.conditions.product"),
    targetAttribute = "product_name2", outputColumn = "legs.conditions.conformed_product_new")

  val lit2Rule = LiteralConformanceRule(order = 7, outputColumn = "ToBeDropped", controlCheckpoint = true,
    value = "123456")

  val dropRule = DropConformanceRule(order = 8, outputColumn = "ToBeDropped", controlCheckpoint = false)

  val concatRule = ConcatenationConformanceRule(order = 9, outputColumn = "Concatenated", controlCheckpoint = true,
    Seq("MyLiteral", "MyUpperLiteral"))

  val singleColRule = SingleColumnConformanceRule(order = 10, outputColumn = "legs.ConformedLegId",
    controlCheckpoint = true, inputColumn = "legs.legid", inputColumnAlias = "legId")

  val tradeDS = Dataset(name = "Trade Conformance", version = 1, hdfsPath = "src/test/testData/_tradeData",
    hdfsPublishPath = "testData/conformedTrade",
    schemaName = "Employee", schemaVersion = 0,
    conformance = List(countryRule, litRule, upperRule, currencyRule, productRule, product2Rule, lit2Rule, dropRule,
      concatRule, singleColRule))

  val tradeInfoFile: String = IOUtils.toString(this.getClass
    .getResourceAsStream("/interpreter/tradeConformance/TradeInfoFile.json"), "UTF-8")

  // The original sample used to create the parquet file
  // import spark.implicits._
  // val dfA = spark.read.json(sample.toDS)
  // dfA.repartition(1).write.parquet("conformance/src/test/testData/trade/2017/11/01/")
  val sample: List[String] =
  """{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"country":"CZE","currency":"Kc","product":"Stock"}]}]}""" ::
    """{"id":2,"legs":[{"legid":200,"conditions":[{"checks":[{"checkNums":["8","9","10b","11","12c","13"]}],"country":"SA","currency":"Rand","product":"Stock"}]}]}""" ::
    """{"id":3,"legs":[{"legid":300,"conditions":[{"checks":[],"country": "SWE","currency":"SWK","product":"Stock"}]},{"legid":301,"conditions":[{"checks":[],"country": "SA","currency":"Dummy","product":"Bond"}]}]}""" ::
    """{"id":4,"legs":[{"legid":400,"conditions":[{"checks":null,"country": "CZE","currency":"Kc","product":"Wrong"}]},{"legid":401,"conditions":[{"checks":null,"country": "SA","currency":"Rand","product":"Stock"}]}]}""" ::
    """{"id":5,"legs":[{"legid":500,"conditions":[]},{"legid":501,"conditions":[]},{"legid":502,"conditions":[]}]}""" ::
    """{"id":6,"legs":[]}""" ::
    """{"id":7}""" :: Nil

  val expectedConformedGroupExplode: String = IOUtils.toString(this.getClass
    .getResourceAsStream("/interpreter/tradeConformance/expectedConformedGroupExplode.json"), "UTF-8")
    .replace("\r\n", "\n")

  val expectedConformedJson: String = IOUtils.toString(this.getClass
    .getResourceAsStream("/interpreter/tradeConformance/expectedConformed.json"), "UTF-8")
    .replace("\r\n", "\n")

  def createTestDataFiles()(implicit spark: SparkSession): Unit = {
    createTradeData()
    createCountryMT()
    createCurrencyMT()
    createProductMT()
  }

  def createTradeData()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val dfA = spark.read.json(sample.toDS)
    dfA
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(getTradeDataPath(tradeDS.hdfsPath))
    FileUtils.writeStringToFile(new File(s"${getTradeDataPath(tradeDS.hdfsPath)}/_INFO"), tradeInfoFile, Charset.defaultCharset)
  }

  def createCountryMT()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    Seq(("CZE", "Czech Republic"), ("SA", "South Africa"), ("IN", "India"), ("DE", "Germany")).toDF
      .select($"_1".as("country_code"), $"_2".as("country_name"))
      .write.mode(SaveMode.Overwrite).parquet(getMappingTablePath(countryMT.hdfsPath))
  }

  def createCurrencyMT()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    Seq(("Kc", "CZK"), ("Rand", "ZAR"), ("SWK", "SEK")).toDF
      .select($"_1".as("currency_code"), $"_2".as("currency_name"))
      .write.mode(SaveMode.Overwrite).parquet(getMappingTablePath(currencyMT.hdfsPath))
  }

  def createProductMT()(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    Seq(("Stock", "STK", "STK2"), ("Bond", "BND", "BND2"))
      .toDF
      .select($"_1".as("product_code"), $"_2".as("product_name"), $"_3".as("product_name2"))
         .write.mode(SaveMode.Overwrite).parquet(getMappingTablePath(productMT.hdfsPath))
  }

  def deleteTestData(): Unit = {
    deleteOutputData()
    deleteTradeData()
    deleteCountryMT()
    deleteCurrencyMT()
    deleteProductMT()
  }

  def deleteOutputData(): Unit = {
    safeDeleteMappingTableDir("src/test/testData/_testOutput")
  }

  def deleteTradeData(): Unit = {
    safeDeleteMappingTableDir(getTradeDataPath(tradeDS.hdfsPath))
  }

  def deleteCountryMT(): Unit = {
    safeDeleteMappingTableDir(countryMT.hdfsPath)
  }

  def deleteCurrencyMT(): Unit = {
    safeDeleteMappingTableDir(currencyMT.hdfsPath)
  }

  def deleteProductMT(): Unit = {
    safeDeleteMappingTableDir(productMT.hdfsPath)
  }

  def safeDeleteMappingTableDir(path: String): Unit = {
    try {
      FileUtils.deleteDirectory(new File(getMappingTablePath(path)))
    } catch {
      case NonFatal(e) => log.warn(s"Unable to delete a test mapping table directory $path")
    }
  }

  private def getTradeDataPath(basePath: String): String = s"$basePath/2017/11/01/"
  private def getMappingTablePath(basePath: String): String = s"$basePath/reportDate=2017-11-01"
}
