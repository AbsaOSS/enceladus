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

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.cbartosiak.bson.codecs.jsr310.zoneddatetime.ZonedDateTimeAsDocumentCodec
import org.bson.BsonDocumentWriter
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.{Codec, EncoderContext}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import za.co.absa.atum.model._
import za.co.absa.enceladus.migrations.migrations.model1.conformanceRule._

/**
  * This is the object for deserializing Model 1 version of Enceladus Schema
  */
object Serializer1 {
  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)

  implicit private val formatsJson: Formats = Serialization.formats(NoTypeHints).withBigDecimal

  private val codecRegistry = fromRegistries(fromProviders(
    classOf[Schema], classOf[SchemaField],
    classOf[MappingTable], classOf[DefaultValue], classOf[MenasReference],
    classOf[Dataset], classOf[ConcatenationConformanceRule],
    classOf[MappingConformanceRule], classOf[LiteralConformanceRule], classOf[ConcatenationConformanceRule],
    classOf[DropConformanceRule], classOf[SparkSessionConfConformanceRule], classOf[UppercaseConformanceRule],
    classOf[SingleColumnConformanceRule], classOf[CastingConformanceRule], classOf[NegationConformanceRule],
    classOf[Run], classOf[SplineReference], classOf[RunStatus], classOf[ControlMeasure],
    classOf[ControlMeasureMetadata], classOf[Checkpoint], classOf[Measurement]),
    CodecRegistries.fromCodecs(new ZonedDateTimeAsDocumentCodec()), DEFAULT_CODEC_REGISTRY)

  private val schemaCodec: Codec[Schema] = codecRegistry.get(classOf[Schema])
  private val mappingTableCodec: Codec[MappingTable] = codecRegistry.get(classOf[MappingTable])
  private val datasetCodec: Codec[Dataset] = codecRegistry.get(classOf[Dataset])
  private val runCodec: Codec[Run] = codecRegistry.get(classOf[Run])

  /**
    * Serializes a Model 1 schema JSON
    */
  def serializeSchema(schema: Schema): String = {
    val bsonDocument = new BsonDocument
    val bsonWriter = new BsonDocumentWriter(bsonDocument)
    val encodeContext = EncoderContext.builder.build

    schemaCodec.encode(bsonWriter, schema, encodeContext)

    bsonDocument.toJson
  }

  /**
    * Deserializes a Model 1 schema JSON
    */
  def deserializeSchema(json: String): Schema = {
    objectMapper.readValue(json, classOf[Schema])
  }

  /**
    * Serializes a Model 1 mapping table JSON
    */
  def serializeMappingTable(mappingTable: MappingTable): String = {
    val bsonDocument = new BsonDocument
    val bsonWriter = new BsonDocumentWriter(bsonDocument)
    val encodeContext = EncoderContext.builder.build

    mappingTableCodec.encode(bsonWriter, mappingTable, encodeContext)

    bsonDocument.toJson
  }

  /**
    * Deserializes a Model 1 mapping table JSON
    */
  def deserializeMappingTable(json: String): MappingTable = {
    objectMapper.readValue(json, classOf[MappingTable])
  }

  /**
    * Serializes a Model 1 dataset JSON
    */
  def serializeDataset(dataset: Dataset): String = {
    val bsonDocument = new BsonDocument
    val bsonWriter = new BsonDocumentWriter(bsonDocument)
    val encodeContext = EncoderContext.builder.build

    datasetCodec.encode(bsonWriter, dataset, encodeContext)

    bsonDocument.toJson
  }

  /**
    * Serializes a Model 1 run JSON
    */
  def serializeRun(run: Run): String = {
    objectMapper.writeValueAsString(run)
  }

  /**
    * Deserializes a Model 1 dataset JSON
    */
  def deserializeDataset(json: String): Dataset = {
    objectMapper.readValue(json, classOf[Dataset])
  }

}
