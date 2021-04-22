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

package za.co.absa.enceladus.migrations.migrations.model1

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

package object conformanceRule {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
  @JsonSubTypes(Array(
    new Type(value = classOf[CastingConformanceRule], name = "CastingConformanceRule"),
    new Type(value = classOf[ConcatenationConformanceRule], name = "ConcatenationConformanceRule"),
    new Type(value = classOf[DropConformanceRule], name = "DropConformanceRule"),
    new Type(value = classOf[LiteralConformanceRule], name = "LiteralConformanceRule"),
    new Type(value = classOf[MappingConformanceRule], name = "MappingConformanceRule"),
    new Type(value = classOf[NegationConformanceRule], name = "NegationConformanceRule"),
    new Type(value = classOf[SingleColumnConformanceRule], name = "SingleColumnConformanceRule"),
    new Type(value = classOf[SparkSessionConfConformanceRule], name = "SparkSessionConfConformanceRule"),
    new Type(value = classOf[UppercaseConformanceRule], name = "UppercaseConformanceRule")
  ))
  sealed trait ConformanceRule {
    val order: Int
    val outputColumn: String
    val controlCheckpoint: Boolean

    def withUpdatedOrder(newOrder: Int): ConformanceRule
  }

  case class ConcatenationConformanceRule(order: Int,
                                          outputColumn: String,
                                          controlCheckpoint: Boolean,
                                          inputColumns: Seq[String],
                                          _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }

  case class CastingConformanceRule(order: Int,
                                    outputColumn: String,
                                    controlCheckpoint: Boolean,
                                    inputColumn: String,
                                    outputDataType: String,
                                    _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }

  case class DropConformanceRule(order: Int,
                                 controlCheckpoint: Boolean,
                                 outputColumn: String,
                                 _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }

  case class LiteralConformanceRule(order: Int,
                                    outputColumn: String,
                                    controlCheckpoint: Boolean,
                                    value: String,
                                    _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }


  case class MappingConformanceRule(order: Int,
                                    controlCheckpoint: Boolean,
                                    mappingTable: String,
                                    mappingTableVersion: Int,
                                    attributeMappings: Map[String, String], // key = mapping table column, value = input df column
                                    additionalColumns: Option[Map[String, String]] = None,
                                    targetAttribute: String,
                                    outputColumn: String,
                                    isNullSafe: Boolean,
                                    _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }

  case class NegationConformanceRule(order: Int,
                                     outputColumn: String,
                                     controlCheckpoint: Boolean,
                                     inputColumn: String,
                                     _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }

  case class SingleColumnConformanceRule(
                                          order: Int,
                                          controlCheckpoint: Boolean,
                                          outputColumn: String,
                                          inputColumn: String,
                                          inputColumnAlias: String,
                                          _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }

  case class SparkSessionConfConformanceRule(order: Int,
                                             outputColumn: String,
                                             controlCheckpoint: Boolean,
                                             sparkConfKey: String,
                                             _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }

  case class UppercaseConformanceRule(order: Int,
                                      outputColumn: String,
                                      controlCheckpoint: Boolean,
                                      inputColumn: String,
                                      _t: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConformanceRule = copy(order = newOrder)
  }

}
