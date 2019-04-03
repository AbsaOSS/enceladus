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

package za.co.absa.enceladus.rest.factories

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.model.menas.MenasReference

object DatasetFactory {

  val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss XXXX")
  val dummyZonedDateTime = ZonedDateTime.parse("04-12-2017 16:19:17 +0200", formatter)

  def getDummyDataset(name: String = "dummyName",
                      version: Int = 1,
                      description: Option[String] = None,
                      hdfsPath: String = "/dummy/path",
                      hdfsPublishPath: String = "/dummy/publish/path",
                      schemaName: String = "dummySchema",
                      schemaVersion: Int = 1,
                      dateCreated: ZonedDateTime = dummyZonedDateTime,
                      userCreated: String = "dummyUser",
                      lastUpdated: ZonedDateTime = dummyZonedDateTime,
                      userUpdated: String = "dummyUser",
                      disabled: Boolean = false,
                      dateDisabled: Option[ZonedDateTime] = None,
                      userDisabled: Option[String] = None,
                      conformance: List[ConformanceRule] = List(),
                      parent: Option[MenasReference] = None): Dataset = {

    Dataset(name, version, description, hdfsPath, hdfsPublishPath, schemaName,
      schemaVersion, dateCreated, userCreated, lastUpdated, userUpdated,
      disabled, dateDisabled, userDisabled, conformance, parent)
  }

  def getDummyConcatenationRule(order: Int = 1,
                                outputColumn: String = "dummyOutputCol",
                                controlCheckpoint: Boolean = true,
                                inputColumns: Seq[String] = Seq()): ConcatenationConformanceRule = {
    ConcatenationConformanceRule(order, outputColumn, controlCheckpoint, inputColumns)
  }

  def getDummyCastingRule(order: Int = 1,
                          outputColumn: String = "dummyOutputCol",
                          controlCheckpoint: Boolean = true,
                          inputColumn: String = "dummyInputCol",
                          outputDataType: String = "StringType"): CastingConformanceRule = {
    CastingConformanceRule(order, outputColumn, controlCheckpoint, inputColumn, outputDataType)
  }

  def getDummyDropRule(order: Int = 1,
                       controlCheckpoint: Boolean = true,
                       outputColumn: String = "dummyOutputCol"): DropConformanceRule = {
    DropConformanceRule(order, controlCheckpoint, outputColumn)
  }

  def getDummyLiteralRule(order: Int = 1,
                          outputColumn: String = "dummyOutputCol",
                          controlCheckpoint: Boolean = true,
                          value: String = "dummyValue"): LiteralConformanceRule = {
    LiteralConformanceRule(order, outputColumn, controlCheckpoint, value)
  }


  def getDummyMappingRule(order: Int = 1,
                          controlCheckpoint: Boolean = true,
                          mappingTable: String = "dummyMappingTable",
                          mappingTableVersion: Int = 1,
                          attributeMappings: Map[String, String] = Map("dummyKey" -> "dummyValue"),
                          targetAttribute: String = "dummyTargetAttribute",
                          outputColumn: String = "dummyOutputCol",
                          isNullSafe: Boolean = false): MappingConformanceRule = {

    MappingConformanceRule(order, controlCheckpoint, mappingTable, mappingTableVersion,
      attributeMappings, targetAttribute, outputColumn, isNullSafe)
  }

  def getDummyNegationRule(order: Int = 1,
                           outputColumn: String = "dummyOutputCol",
                           controlCheckpoint: Boolean = true,
                           inputColumn: String = "dummyInputCol"): NegationConformanceRule = {
    NegationConformanceRule(order, outputColumn, controlCheckpoint, inputColumn)
  }

  def getDummySingleColumnRule(order: Int = 1,
                               controlCheckpoint: Boolean = true,
                               outputColumn: String = "dummyOutputCol",
                               inputColumn: String = "dummyInputCol",
                               inputColumnAlias: String = "dummyInputColAlias"): SingleColumnConformanceRule = {
    SingleColumnConformanceRule(order, controlCheckpoint, outputColumn, inputColumn, inputColumnAlias)
  }

  def getDummySparkSessionConfRule(order: Int = 1,
                                   outputColumn: String = "dummyOutputCol",
                                   controlCheckpoint: Boolean = true,
                                   sparkConfKey: String = "dummy.spark.conf.key"): SparkSessionConfConformanceRule = {
    SparkSessionConfConformanceRule(order, outputColumn, controlCheckpoint, sparkConfKey)
  }

  def getDummyUppercaseRule(order: Int = 1,
                            outputColumn: String = "dummyOutputCol",
                            controlCheckpoint: Boolean = true,
                            inputColumn: String = "dummyInputCol"): UppercaseConformanceRule = {
    UppercaseConformanceRule(order, outputColumn, controlCheckpoint, inputColumn)
  }

  def getDummyDatasetParent(collection: Option[String] = Some("dataset"),
                            name: String = "dataset",
                            version: Int = 1): MenasReference = {
    MenasReference(collection, name, version)
  }

}
