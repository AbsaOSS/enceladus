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

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats, NoTypeHints, ext}
import za.co.absa.atum.model.RunState

/**
  * This is the object for deserializing Model 0 version of Enceladus Schema
  */
object Serializer0 {
  private implicit val formatsDefault: Formats = DefaultFormats.withHints(NoTypeHints).withBigDecimal +
    new ext.EnumNameSerializer(RunState)

  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)

  /**
    * Deserializes a Model 0 schema JSON
    */
  def deserializeSchema(json: String): Schema = {
    Serialization.read[Schema](json)
  }

  /**
    * Deserializes a Model 0 mapping table JSON
    */
  def deserializeMappingTable(json: String): MappingTable = {
    Serialization.read[MappingTable](json)
  }

  /**
    * Deserializes a Model 0 dataset JSON
    */
  def deserializeDataset(json: String): Dataset = {
    objectMapper.readValue(json, classOf[Dataset])
  }

  /**
    * Deserializes a Model 0 run JSON
    */
  def deserializeRun(json: String): Run = {
    Serialization.read[Run](json)
  }

  /**
    * Serializes a Model 0 run object
    */
  def serializeRun(run: Run): String = {
    Serialization.write[Run](run)
  }

}
