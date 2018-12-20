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

package za.co.absa.enceladus.model.conformanceRule

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

//@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
//@JsonSubTypes(Array(
//   new Type(value = classOf[CastingConformanceRule], name = "CastingConformanceRule"),
//   new Type(value = classOf[ConcatenationConformanceRule], name = "ConcatenationConformanceRule"),
//   new Type(value = classOf[DropConformanceRule], name = "DropConformanceRule"),
//   new Type(value = classOf[LiteralConformanceRule], name = "LiteralConformanceRule"),
//   new Type(value = classOf[MappingConformanceRule], name = "MappingConformanceRule"),
//   new Type(value = classOf[NegationConformanceRule], name = "NegationConformanceRule"),
//   new Type(value = classOf[SingleColumnConformanceRule], name = "SingleColumnConformanceRule"),
//   new Type(value = classOf[SparkSessionConfConformanceRule], name = "SparkSessionConfConformanceRule"),
//   new Type(value = classOf[UppercaseConformanceRule], name = "UppercaseConformanceRule")
//))
//sealed class ConformanceRule(val order: Int,
//                             val outputColumn: String,
//                             val controlCheckpoint: Boolean) {
//}
//
//case class ConcatenationConformanceRule(override val order: Int,
//                                        override val outputColumn: String,
//                                        override val controlCheckpoint: Boolean,
//                                        inputColumns: Seq[String]) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//case class CastingConformanceRule(override val order: Int,
//                                  override val outputColumn: String,
//                                  override val controlCheckpoint: Boolean,
//                                  inputColumn: String,
//                                  outputDataType: String) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//case class DropConformanceRule(override val order: Int,
//                               override val controlCheckpoint: Boolean,
//                               override val outputColumn: String) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//case class LiteralConformanceRule(override val order: Int,
//                                  override val outputColumn: String,
//                                  override val controlCheckpoint: Boolean,
//                                  value: String) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//
//case class MappingConformanceRule(override val order: Int,
//                                  override val controlCheckpoint: Boolean,
//                                  mappingTable: String,
//                                  mappingTableVersion: Int,
//                                  attributeMappings: Map[String, String], // key = mapping table column, value = input df column
//                                  targetAttribute: String,
//                                  override val outputColumn: String,
//                                  isNullSafe: Boolean = false) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//case class NegationConformanceRule(override val order: Int,
//                                   override val outputColumn: String,
//                                   override val controlCheckpoint: Boolean,
//                                   inputColumn: String) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//case class SingleColumnConformanceRule(override val order: Int,
//                                       override val controlCheckpoint: Boolean,
//                                       override val outputColumn: String,
//                                       inputColumn: String,
//                                       inputColumnAlias: String) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//case class SparkSessionConfConformanceRule(override val order: Int,
//                                           override val outputColumn: String,
//                                           override val controlCheckpoint: Boolean,
//                                           sparkConfKey: String) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//case class UppercaseConformanceRule(override val order: Int,
//                                    override val outputColumn: String,
//                                    override val controlCheckpoint: Boolean,
//                                    inputColumn: String) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//class ExtensibleConformanceRule(override val order: Int,
//                                override val outputColumn: String,
//                                override val controlCheckpoint: Boolean) extends ConformanceRule(order, outputColumn, controlCheckpoint)
//
//object MappingConformanceRule {
//  // attributeMappings property has key's with dot's that mongo doesn't accept; this symbol is used to replace the dots
//  val DOT_REPLACEMENT_SYMBOL: Char = '^'
//}
