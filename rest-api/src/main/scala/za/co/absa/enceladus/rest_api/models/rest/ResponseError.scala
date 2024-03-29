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

package za.co.absa.enceladus.rest_api.models.rest

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import za.co.absa.enceladus.rest_api.models.rest.errors.{SchemaFormatError, SchemaParsingError}

/**
  * This abstract class is used as a parent for all REST errors.
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
@JsonSubTypes(Array(
  new Type(value = classOf[SchemaFormatError], name = "SchemaFormatError"),
  new Type(value = classOf[SchemaParsingError], name = "SchemaParsingError")
))
abstract class ResponseError {
  /** An error type. Should be as concise as possible to be parsable by a service. */
  val errorType: String
}
