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

import za.co.absa.enceladus.menas.models.rest.exceptions.SchemaFormatException

object SchemaType extends Enumeration {
  val Struct: SchemaType.Value = Value("struct")
  val Copybook: SchemaType.Value = Value("copybook")
  val Avro: SchemaType.Value = Value("avro")

  /**
   * Conversion from a string-based name to [[SchemaType.Value]] that throws correct [[SchemaFormatException]] on error.
   * Basically, this is [[SchemaType#withName]] with exception wrapping
   *
   * @param schemaName string-based schema name
   * @return converted schema format
   * @throws SchemaFormatException if a schema not resolvable by [[SchemaType#withName]] is supplied
   */
  def fromSchemaName(schemaName: String): SchemaType.Value =
    try {
      SchemaType.withName(schemaName)
    } catch {
      case e: NoSuchElementException =>
        throw SchemaFormatException(schemaName, s"'$schemaName' is not a recognized schema format. " +
          s"Menas currently supports: ${SchemaType.values.mkString(", ")}.", cause = e)
    }

  /**
   * Conversion from a optional-string-based name to [[SchemaType.Value]] that throws correct [[SchemaFormatException]] on invalid.
   *
   * @param optSchemaName Option of the schema name
   * @return SchemaType.Value instance based on the name; with special cases: `Some("")` and `None` -> [[SchemaType.Struct]]
   * @throws SchemaFormatException if a schema not resolvable by [[SchemaType#withName]] is supplied or it is not one of the special cases
   */
  def fromOptSchemaName(optSchemaName: Option[String]): SchemaType.Value = optSchemaName match {
    case Some("") | None => SchemaType.Struct // legacy compatibility cases
    case Some(schemaName) => SchemaType.fromSchemaName(schemaName)
  }
}
