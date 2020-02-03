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

package za.co.absa.enceladus.model.api.versionedModelDetail

import za.co.absa.enceladus.model.Schema

/**
  * @param model the schema that was requested
  * @param schemaFormatted will be populated as a JSON string if a specific format is requested
  */
case class SchemaDetail
(
  model: Schema,

  schemaFormatted: Option[String]
) extends VersionedModelDetail[Schema]
