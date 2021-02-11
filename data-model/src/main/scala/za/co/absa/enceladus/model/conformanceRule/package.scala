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

package za.co.absa.enceladus.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import za.co.absa.enceladus.model.dataFrameFilter.DataFrameFilter

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
    new Type(value = classOf[UppercaseConformanceRule], name = "UppercaseConformanceRule"),
    new Type(value = classOf[FillNullsConformanceRule], name = "FillNullsConformanceRule"),
    new Type(value = classOf[CoalesceConformanceRule], name = "CoalesceConformanceRule")
  ))
  sealed trait ConformanceRule {
    val order: Int
    val outputColumn: String
    val controlCheckpoint: Boolean

    def withUpdatedOrder(newOrder: Int): ConformanceRule
    def connectedEntities: Seq[ConnectedEntity] = Seq.empty
    def hasConnectedEntities: Boolean = connectedEntities.nonEmpty
  }

  case class ConcatenationConformanceRule(order: Int,
                                          outputColumn: String,
                                          controlCheckpoint: Boolean,
                                          inputColumns: Seq[String]) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConcatenationConformanceRule = copy(order = newOrder)
  }

  case class CastingConformanceRule(order: Int,
                                    outputColumn: String,
                                    controlCheckpoint: Boolean,
                                    inputColumn: String,
                                    outputDataType: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): CastingConformanceRule = copy(order = newOrder)
  }

  case class DropConformanceRule(order: Int,
                                 controlCheckpoint: Boolean,
                                 outputColumn: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): DropConformanceRule = copy(order = newOrder)
  }

  case class LiteralConformanceRule(order: Int,
                                    outputColumn: String,
                                    controlCheckpoint: Boolean,
                                    value: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): LiteralConformanceRule = copy(order = newOrder)
  }


  case class MappingConformanceRule(order: Int,
                                    controlCheckpoint: Boolean,
                                    mappingTable: String,
                                    mappingTableVersion: Int,
                                    attributeMappings: Map[String, String], // key = mapping table column, value = input df column
                                    targetAttribute: String,
                                    outputColumn: String,
                                    additionalColumns: Option[Map[String, String]] = None,
                                    isNullSafe: Boolean = false,
                                    mappingTableFilter: Option[DataFrameFilter] = None,
                                    overrideMappingTableOwnFilter: Boolean = false
                                   ) extends ConformanceRule {

    def getAllOutputColumns(): Map[String, String] = {
      additionalColumns.getOrElse(Map()) + (outputColumn -> targetAttribute)
    }

    override def withUpdatedOrder(newOrder: Int): MappingConformanceRule = copy(order = newOrder)

    override def connectedEntities: Seq[ConnectedEntity] = Seq(
      ConnectedMappingTable(mappingTable, mappingTableVersion)
    )
  }

  case class NegationConformanceRule(order: Int,
                                     outputColumn: String,
                                     controlCheckpoint: Boolean,
                                     inputColumn: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): NegationConformanceRule = copy(order = newOrder)
  }

  /**
    * Used to create a conformed struct out of a single column
    *
    * inputColumn -> struct(inputColumn as inputColumnAlias) as outputColumn
    */
  case class SingleColumnConformanceRule(order: Int,
                                         controlCheckpoint: Boolean,
                                         outputColumn: String,
                                         inputColumn: String,
                                         inputColumnAlias: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): SingleColumnConformanceRule = copy(order = newOrder)
  }

  /**
    * Rule for getting values out of spark session conf.
    *
    * This is an easy way of introducing values from the info file into the dataset (such as version), where control
    * framework will populate the conf.
    *
    * Gets value from spark.sessionState.conf
    */
  case class SparkSessionConfConformanceRule(order: Int,
                                             outputColumn: String,
                                             controlCheckpoint: Boolean,
                                             sparkConfKey: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): SparkSessionConfConformanceRule = copy(order = newOrder)
  }

  case class UppercaseConformanceRule(order: Int,
                                      outputColumn: String,
                                      controlCheckpoint: Boolean,
                                      inputColumn: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): UppercaseConformanceRule = copy(order = newOrder)
  }

  case class FillNullsConformanceRule(order: Int,
                                      outputColumn: String,
                                      controlCheckpoint: Boolean,
                                      inputColumn: String,
                                      value: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): FillNullsConformanceRule = copy(order = newOrder)
  }

  case class CoalesceConformanceRule(order: Int,
                                     outputColumn: String,
                                     controlCheckpoint: Boolean,
                                     inputColumns: Seq[String]) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): CoalesceConformanceRule = copy(order = newOrder)
  }

  abstract class ExtensibleConformanceRule() extends ConformanceRule

  object MappingConformanceRule {
    // attributeMappings property has key's with dot's that mongo doesn't accept; this symbol is used to replace the dots
    val DOT_REPLACEMENT_SYMBOL: Char = '^'
  }

}
