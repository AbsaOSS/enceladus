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

package za.co.absa.enceladus.migrations.migrations.model0

package object conformanceRule {

  sealed trait ConformanceRule {
    val order: Int
    val outputColumn: String
    val controlCheckpoint: Boolean
  }
  case class CastingConformanceRule(
                                     order: Int,
                                     outputColumn: String,
                                     controlCheckpoint: Boolean,
                                     inputColumn: String,
                                     outputDataType: String) extends ConformanceRule

  case class ConcatenationConformanceRule(
                                           order: Int,
                                           outputColumn: String,
                                           controlCheckpoint: Boolean,
                                           inputColumns: String*) extends ConformanceRule

  case class DropConformanceRule(
                                  order: Int,
                                  controlCheckpoint: Boolean,
                                  outputColumn: String) extends ConformanceRule

  case class LiteralConformanceRule(
                                     order: Int,
                                     outputColumn: String,
                                     controlCheckpoint: Boolean,
                                     value: String) extends ConformanceRule

  case class MappingConformanceRule(
                                     order: Int,
                                     controlCheckpoint: Boolean,
                                     mappingTable: String,
                                     mappingTableVersion: Int,
                                     attributeMappings: Map[String, String], //key = mapping table column, value = input df column
                                     targetAttribute: String,
                                     outputColumn: String,
                                     isNullSafe: Boolean = false) extends ConformanceRule

  case class NegationConformanceRule(
                                      order: Int,
                                      outputColumn: String,
                                      controlCheckpoint: Boolean,
                                      inputColumn: String) extends ConformanceRule

  case class SingleColumnConformanceRule(
                                          order: Int,
                                          controlCheckpoint: Boolean,
                                          outputColumn: String,
                                          inputColumn: String,
                                          inputColumnAlias: String) extends ConformanceRule

  case class SparkSessionConfConformanceRule(
                                              order: Int,
                                              outputColumn: String,
                                              controlCheckpoint: Boolean,
                                              sparkConfKey: String) extends ConformanceRule

  case class UppercaseConformanceRule(
                                       order: Int,
                                       outputColumn: String,
                                       controlCheckpoint: Boolean,
                                       inputColumn: String) extends ConformanceRule

}
