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

package za.co.absa.enceladus.rest_api.models.rest.errors

import za.co.absa.enceladus.rest_api.models.rest.ResponseError
import za.co.absa.enceladus.rest_api.models.rest.exceptions.SchemaParsingException
import za.co.absa.enceladus.rest_api.utils.SchemaType

/**
  * This error is produced when a parsing error is occurred when uploading a schema.
  */
case class SchemaParsingError(
                               errorType: String,
                               schemaType: SchemaType.Value,
                               line: Option[Int],
                               column: Option[Int],
                               field: Option[String]
                             ) extends ResponseError

object SchemaParsingError {
  def fromException(ex: SchemaParsingException): SchemaParsingError = SchemaParsingError(
    errorType = "schema_parsing",
    schemaType = ex.schemaType,
    line = ex.line,
    column = ex.column,
    field = ex.field)
}
