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
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo, JsonTypeName}
import za.co.absa.enceladus.model.dataFrameFilter.DataFrameFilter
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule.DefaultOverrideMappingTableOwnFilter
import io.swagger.v3.oas.annotations.media.{ArraySchema, DiscriminatorMapping, Schema => AosSchema}

import scala.annotation.meta.field
import scala.beans.BeanProperty

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
  @AosSchema(
    description = "Conformance rule",
    discriminatorProperty = "_t",
    discriminatorMapping = Array(
      new DiscriminatorMapping(value = "CastingConformanceRule", schema = classOf[CastingConformanceRule]),
      new DiscriminatorMapping(value = "ConcatenationConformanceRule", schema = classOf[ConcatenationConformanceRule]),
      new DiscriminatorMapping(value = "DropConformanceRule", schema = classOf[DropConformanceRule]),
      new DiscriminatorMapping(value = "LiteralConformanceRule", schema = classOf[LiteralConformanceRule]),
      new DiscriminatorMapping(value = "MappingConformanceRule", schema = classOf[MappingConformanceRule]),
      new DiscriminatorMapping(value = "NegationConformanceRule", schema = classOf[NegationConformanceRule]),
      new DiscriminatorMapping(value = "SingleColumnConformanceRule", schema = classOf[SingleColumnConformanceRule]),
      new DiscriminatorMapping(value = "SparkSessionConfConformanceRule", schema = classOf[SparkSessionConfConformanceRule]),
      new DiscriminatorMapping(value = "UppercaseConformanceRule", schema = classOf[UppercaseConformanceRule]),
      new DiscriminatorMapping(value = "FillNullsConformanceRule", schema = classOf[FillNullsConformanceRule]),
      new DiscriminatorMapping(value = "CoalesceConformanceRule", schema = classOf[CoalesceConformanceRule])
  ))
  sealed trait ConformanceRule {
    val order: Int
    val outputColumn: String
    val controlCheckpoint: Boolean

    def withUpdatedOrder(newOrder: Int): ConformanceRule
    def connectedEntities: Seq[ConnectedEntity] = Seq.empty
    def hasConnectedEntities: Boolean = connectedEntities.nonEmpty
  }

  case class ConcatenationConformanceRule(
    @BeanProperty order: Int,
    @(AosSchema@field)(example = "fullnameColumn")
    @BeanProperty outputColumn: String,
    @BeanProperty controlCheckpoint: Boolean,
    @(ArraySchema@field)(schema = new AosSchema(example = "[\"firstnameColumn\", \"lastnameColumn\"]"))
    @BeanProperty inputColumns: Seq[String]) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): ConcatenationConformanceRule = copy(order = newOrder)
  }

  case class CastingConformanceRule(
    @BeanProperty order: Int,
    @(AosSchema@field)(example = "conformedInt")
    @BeanProperty outputColumn: String,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "stringInt1")
    @BeanProperty inputColumn: String,
    @(AosSchema@field)(example = "conformedInt")
    @BeanProperty outputDataType: String
  ) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): CastingConformanceRule = copy(order = newOrder)
  }

  case class DropConformanceRule(
    @BeanProperty order: Int,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "toBeDroppedCol1")
    @BeanProperty outputColumn: String
  ) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): DropConformanceRule = copy(order = newOrder)
  }

  case class LiteralConformanceRule(
    @BeanProperty order: Int,
    @(AosSchema@field)(example = "ConformedLiteral")
    @BeanProperty outputColumn: String,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "AAA")
    @BeanProperty value: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): LiteralConformanceRule = copy(order = newOrder)
  }

  case class MappingConformanceRule(
    @BeanProperty order: Int,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "CurrencyMappingTable2")
    @BeanProperty mappingTable: String,
    @(AosSchema@field)(example = "3")
    @BeanProperty mappingTableVersion: Int,

    @(AosSchema@field)(implementation = classOf[java.util.Map[String, String]], example = "{\"field1\": \"mappedField1\"}")
    @BeanProperty attributeMappings: Map[String, String], // key = mapping table column, value = input df column
    @(AosSchema@field)(example = "CCC")
    @BeanProperty targetAttribute: String,
    @(AosSchema@field)(example = "ConformedCurrencyX")
    @BeanProperty outputColumn: String,

    @(AosSchema@field)(implementation = classOf[java.util.Map[String, String]], example = "{\"newCol\": \"mappedCol1\"}")
    @BeanProperty additionalColumns: Option[Map[String, String]] = None,
    @BeanProperty isNullSafe: Boolean = false,

    @(AosSchema@field)(implementation = classOf[DataFrameFilter],
      example = "{\"_t\": \"EqualsFilter\", \"columnName\": \"column1\", \"value\": \"soughtAfterValue\", \"valueType\": \"string\"}")
    @BeanProperty mappingTableFilter: Option[DataFrameFilter] = None,
    overrideMappingTableOwnFilter: Option[Boolean] = Some(DefaultOverrideMappingTableOwnFilter)
  ) extends ConformanceRule {

    def allOutputColumns(): Map[String, String] = {
      definedAdditionalColumns() + (outputColumn -> targetAttribute)
    }

    def definedAdditionalColumns(): Map[String, String] = additionalColumns.getOrElse(Map())

    override def withUpdatedOrder(newOrder: Int): MappingConformanceRule = copy(order = newOrder)

    override def connectedEntities: Seq[ConnectedEntity] = Seq(
      ConnectedMappingTable(mappingTable, mappingTableVersion)
    )

    def getOverrideMappingTableOwnFilter: Boolean = {
      overrideMappingTableOwnFilter.getOrElse(DefaultOverrideMappingTableOwnFilter)
    }
  }

  case class NegationConformanceRule(
    @BeanProperty order: Int,
    @(AosSchema@field)(example = "negated_bool_field3")
    @BeanProperty outputColumn: String,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "bool_field3")
    @BeanProperty inputColumn: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): NegationConformanceRule = copy(order = newOrder)
  }

  /**
    * Used to create a conformed struct out of a single column
    *
    * inputColumn -> struct(inputColumn as inputColumnAlias) as outputColumn
    */
  case class SingleColumnConformanceRule(
    @BeanProperty order: Int,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "single_string1")
    @BeanProperty outputColumn: String,
    @(AosSchema@field)(example = "string1")
    @BeanProperty inputColumn: String,
    @(AosSchema@field)(example = "string_alias1")
    @BeanProperty inputColumnAlias: String) extends ConformanceRule {
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
  case class SparkSessionConfConformanceRule(
    @BeanProperty order: Int,
    @(AosSchema@field)(example = "ssc_output_col1")
    @BeanProperty outputColumn: String,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "a.key.here")
    @BeanProperty sparkConfKey: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): SparkSessionConfConformanceRule = copy(order = newOrder)
  }

  case class UppercaseConformanceRule(
    @BeanProperty order: Int,
    @(AosSchema@field)(example = "uppercased_field1")
    @BeanProperty outputColumn: String,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "field1")
    @BeanProperty inputColumn: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): UppercaseConformanceRule = copy(order = newOrder)
  }

  case class FillNullsConformanceRule(
    @BeanProperty order: Int,
    @(AosSchema@field)(example = "nonnulled_field1")
    @BeanProperty outputColumn: String,
    @BeanProperty controlCheckpoint: Boolean,
    @(AosSchema@field)(example = "field1")
    @BeanProperty inputColumn: String,
    @(AosSchema@field)(example = "I_am_the_null_replacement")
    @BeanProperty value: String) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): FillNullsConformanceRule = copy(order = newOrder)
  }

  case class CoalesceConformanceRule(
    @BeanProperty order: Int,
    @(AosSchema@field)(example = "greeting_name")
    @BeanProperty outputColumn: String,
    @BeanProperty controlCheckpoint: Boolean,
    @(ArraySchema@field)(schema = new AosSchema(example = "[\"nickname\", \"firstname\", \"another_possible_backup\"]"))
    @BeanProperty inputColumns: Seq[String]
  ) extends ConformanceRule {
    override def withUpdatedOrder(newOrder: Int): CoalesceConformanceRule = copy(order = newOrder)
  }

  abstract class ExtensibleConformanceRule() extends ConformanceRule

  object MappingConformanceRule {
    // attributeMappings property has key's with dot's that mongo doesn't accept; this symbol is used to replace the dots
    final val DotReplacementSymbol: Char = '^'
    final val DefaultOverrideMappingTableOwnFilter: Boolean = false
  }

}
