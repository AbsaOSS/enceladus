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

package za.co.absa.enceladus.menas.utils

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import za.co.absa.enceladus.menas.models.rest.exceptions.SchemaFormatException


@JsonSerialize(using = classOf[DirectionJsonSerializer])
@JsonDeserialize(using = classOf[DirectionJsonDeserializer])
sealed trait SchemaType {
  def name: String
}

object SchemaType {

  case object Struct extends SchemaType {
    val name = "struct"
  }

  case object Copybook extends SchemaType {
    val name = "copybook"
  }

  case object Avro extends SchemaType {
    val name = "avro"
  }

  private val values = Seq(Struct, Copybook, Avro)

  /**
   * Enum-like `withName` with exception wrapping
   * @param name name of the schema to be converted to [[SchemaType]] object
   * @return converted schema format
   * @throws SchemaFormatException if a schema not resolvable by [[SchemaType#withName]] is supplied
   */
  def withName(name: String): SchemaType = name match {
    case Struct.name => SchemaType.Struct
    case Copybook.name => SchemaType.Copybook
    case Avro.name => SchemaType.Avro
    case schemaName =>
      throw SchemaFormatException(schemaName, s"'$schemaName' is not a recognized schema format. " +
        s"Menas currently supports: ${SchemaType.values.mkString(", ")}.")
  }

  /**
   * Conversion from a sting-based name to [[SchemaType]] that throws correct [[SchemaFormatException]] on invalid.
   *
   * @param optSchemaName Option of the schema name
   * @return SchemaType instance based on the name; with special cases: empty string and None -> [[SchemaType.Struct]]
   */
  def fromOptSchemaName(optSchemaName: Option[String]): SchemaType = optSchemaName match {
    case Some("") | None => SchemaType.Struct // legacy compatibility cases
    case Some(schemaName) => SchemaType.withName(schemaName)
  }

}

// needed because of the case objects instead of "classic" case classes:
// https://doc.akka.io/docs/akka/current/serialization-jackson.html#adt-with-trait-and-case-object
class DirectionJsonSerializer extends StdSerializer[SchemaType](classOf[SchemaType]) {

  override def serialize(value: SchemaType, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    gen.writeString(value.name)
  }
}

class DirectionJsonDeserializer extends StdDeserializer[SchemaType](classOf[SchemaType]) {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): SchemaType = {
    SchemaType.withName(p.getText)
  }
}
