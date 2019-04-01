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

package za.co.absa.enceladus.samples

import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.model.{Dataset, DefaultValue, MappingTable}
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.error.Mapping

object TradeConformance {
  val countryMT = MappingTable(name = "country", version = 0, hdfsPath = "src/test/testData/country",
    schemaName = "country", schemaVersion = 0)

  // Seq(("Kc", "CZK"), ("Rand", "ZAR"), ("SWK","SEK")).toDF
  // .select($"_1".as("currency_code"), $"_2".as("currency_name"))
  // .write.parquet("src/test/testData/currency/reportDate=2017-11-01")
  val currencyMT = MappingTable(name = "currency", version = 0, hdfsPath = "src/test/testData/currency",
    schemaName = "currency", schemaVersion = 0)

  // Seq(("Stock", "STK"), ("Bond", "BND")).toDF.select($"_1".as("product_code"), $"_2".as("product_name"))
  // .write.parquet("src/test/testData/product/reportDate=2017-11-01")
  val productMT = MappingTable(name = "product", version = 0, hdfsPath = "src/test/testData/product",
    schemaName = "product", schemaVersion = 0)

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


  val lit2Rule = LiteralConformanceRule(order = 6, outputColumn = "ToBeDropped", controlCheckpoint = true,
    value = "123456")

  val dropRule = DropConformanceRule(order = 7, outputColumn = "ToBeDropped", controlCheckpoint = false)

  val concatRule = ConcatenationConformanceRule(order = 8, outputColumn = "Concatenated", controlCheckpoint = true,
    Seq("MyLiteral", "MyUpperLiteral"))

  val singleColRule = SingleColumnConformanceRule(order = 9, outputColumn = "legs.ConformedLegId",
    controlCheckpoint = true, inputColumn = "legs.legid", inputColumnAlias = "legId")

  val tradeDS = Dataset(name = "Trade Conformance", version = 1, hdfsPath = "src/test/testData/trade",
    hdfsPublishPath = "testData/conformedTrade",
    schemaName = "Employee", schemaVersion = 0,
    conformance = List(countryRule, litRule, upperRule, currencyRule, productRule, lit2Rule, dropRule,
      concatRule, singleColRule))

  // The original sample used to create the parquet file
  // import spark.implicits._
  // val dfA = spark.read.json(sample.toDS)
  // dfA.repartition(1).write.parquet("conformance/src/test/testData/trade/2017/11/01/")
  val sample: List[String] =
  """{"id":1,"legs":[{"legid":100,"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"country":"CZE","currency":"Kc","product":"Stock"}]}]}""" ::
    """{"id":2,"legs":[{"legid":200,"conditions":[{"checks":[{"checkNums":["8","9","10b","11","12c","13"]}],"country":"SA","currency":"Rand","product":"Stock"}]}]}""" ::
    """{"id":3,"legs":[{"legid":300,"conditions":[{"checks":[],"country": "SWE","currency":"SWK","product":"Stock"}]},{"legid":301,"conditions":[{"checks":[],"country": "SA","currency":"Rand","product":"Bond"}]}]}""" ::
    """{"id":4,"legs":[{"legid":400,"conditions":[{"checks":null,"country": "CZE","currency":"Kc","product":"Bond"}]},{"legid":401,"conditions":[{"checks":null,"country": "SA","currency":"Rand","product":"Stock"}]}]}""" ::
    """{"id":5,"legs":[{"legid":500,"conditions":[]},{"legid":501,"conditions":[]},{"legid":502,"conditions":[]}]}""" ::
    """{"id":6,"legs":[]}""" ::
    """{"id":7}""" :: Nil

  val expectedConformedJsonNoExplode: List[String] =
    """{"id":1,"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"legs":[{"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"country":"CZE","currency":"Kc","product":"Stock","conformed_country":"Czech Republic","conformed_currency":"CZK","conformed_product":"STK"}],"legid":100,"ConformedLegId":{"legId":100}}],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":2,"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"legs":[{"conditions":[{"checks":[{"checkNums":["8","9","10b","11","12c","13"]}],"country":"SA","currency":"Rand","product":"Stock","conformed_country":"South Africa","conformed_currency":"ZAR","conformed_product":"STK"}],"legid":200,"ConformedLegId":{"legId":200}}],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":3,"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_country","rawValues":["SWE"],"mappings":[{"mappingTableColumn":"country_code","mappedDatasetColumn":"legs.conditions.country"}]}],"legs":[{"conditions":[{"checks":[],"country":"SWE","currency":"SWK","product":"Stock","conformed_currency":"SEK","conformed_product":"STK"}],"legid":300,"ConformedLegId":{"legId":300}},{"conditions":[{"checks":[],"country":"SA","currency":"Rand","product":"Bond","conformed_country":"South Africa","conformed_currency":"ZAR","conformed_product":"BND"}],"legid":301,"ConformedLegId":{"legId":301}}],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":4,"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"legs":[{"conditions":[{"country":"CZE","currency":"Kc","product":"Bond","conformed_country":"Czech Republic","conformed_currency":"CZK","conformed_product":"BND"}],"legid":400,"ConformedLegId":{"legId":400}},{"conditions":[{"country":"SA","currency":"Rand","product":"Stock","conformed_country":"South Africa","conformed_currency":"ZAR","conformed_product":"STK"}],"legid":401,"ConformedLegId":{"legId":401}}],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":5,"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"legs":[{"conditions":[],"legid":500,"ConformedLegId":{"legId":500}},{"conditions":[],"legid":501,"ConformedLegId":{"legId":501}},{"conditions":[],"legid":502,"ConformedLegId":{"legId":502}}],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":6,"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"legs":[],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":7,"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"Concatenated":"abcdefABCDEF"}""" :: Nil

  val expectedConformedJsonWithExplode: List[String] =
    """{"id":1,"legs":[{"conditions":[{"checks":[{"checkNums":["1","2","3b","4","5c","6"]}],"country":"CZE","currency":"Kc","product":"Stock","conformed_country":"Czech Republic","conformed_currency":"CZK","conformed_product":"STK"}],"legid":100,"ConformedLegId":{"legId":100}}],"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":2,"legs":[{"conditions":[{"checks":[{"checkNums":["8","9","10b","11","12c","13"]}],"country":"SA","currency":"Rand","product":"Stock","conformed_country":"South Africa","conformed_currency":"ZAR","conformed_product":"STK"}],"legid":200,"ConformedLegId":{"legId":200}}],"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":3,"legs":[{"conditions":[{"checks":[],"country":"SWE","currency":"SWK","product":"Stock","conformed_currency":"SEK","conformed_product":"STK"}],"legid":300,"ConformedLegId":{"legId":300}},{"conditions":[{"checks":[],"country":"SA","currency":"Rand","product":"Bond","conformed_country":"South Africa","conformed_currency":"ZAR","conformed_product":"BND"}],"legid":301,"ConformedLegId":{"legId":301}}],"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_country","rawValues":["SWE"],"mappings":[{"mappingTableColumn":"country_code","mappedDatasetColumn":"legs.conditions.country"}]}],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":4,"legs":[{"conditions":[{"country":"CZE","currency":"Kc","product":"Bond","conformed_country":"Czech Republic","conformed_currency":"CZK","conformed_product":"BND"}],"legid":400,"ConformedLegId":{"legId":400}},{"conditions":[{"country":"SA","currency":"Rand","product":"Stock","conformed_country":"South Africa","conformed_currency":"ZAR","conformed_product":"STK"}],"legid":401,"ConformedLegId":{"legId":401}}],"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":5,"legs":[{"conditions":[],"legid":500,"ConformedLegId":{"legId":500}},{"conditions":[],"legid":501,"ConformedLegId":{"legId":501}},{"conditions":[],"legid":502,"ConformedLegId":{"legId":502}}],"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_country","rawValues":[null],"mappings":[{"mappingTableColumn":"country_code","mappedDatasetColumn":"legs.conditions.country"}]},{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_currency","rawValues":[null],"mappings":[{"mappingTableColumn":"currency_code","mappedDatasetColumn":"legs.conditions.currency"}]},{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_product","rawValues":[null],"mappings":[{"mappingTableColumn":"product_code","mappedDatasetColumn":"legs.conditions.product"}]}],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":6,"legs":[],"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_country","rawValues":[null],"mappings":[{"mappingTableColumn":"country_code","mappedDatasetColumn":"legs.conditions.country"}]},{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_currency","rawValues":[null],"mappings":[{"mappingTableColumn":"currency_code","mappedDatasetColumn":"legs.conditions.currency"}]},{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_product","rawValues":[null],"mappings":[{"mappingTableColumn":"product_code","mappedDatasetColumn":"legs.conditions.product"}]}],"Concatenated":"abcdefABCDEF"}""" ::
      """{"id":7,"MyLiteral":"abcdef","MyUpperLiteral":"ABCDEF","errCol":[{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_country","rawValues":[null],"mappings":[{"mappingTableColumn":"country_code","mappedDatasetColumn":"legs.conditions.country"}]},{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_currency","rawValues":[null],"mappings":[{"mappingTableColumn":"currency_code","mappedDatasetColumn":"legs.conditions.currency"}]},{"errType":"confMapError","errCode":"E00001","errMsg":"Conformance Error - Null produced by mapping conformance rule","errCol":"legs.conditions.conformed_product","rawValues":[null],"mappings":[{"mappingTableColumn":"product_code","mappedDatasetColumn":"legs.conditions.product"}]}],"Concatenated":"abcdefABCDEF"}""" :: Nil
}
