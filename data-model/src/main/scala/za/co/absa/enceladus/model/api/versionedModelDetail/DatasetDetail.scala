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

package za.co.absa.enceladus.model.api.versionedModelDetail

import za.co.absa.enceladus.model.{Dataset, Schema}

/**
  * @param model the dataset that was requested
  * @param schema will be populated if no formatter is specified
  * @param schemaFormatted will be populated as a JSON string if a specific format is requested
  */
case class DatasetDetail
(
  model: Dataset,

  schema: Option[Schema],
  schemaFormatted: Option[String]
) extends VersionedModelDetail[Dataset] {
  def encode: DatasetDetail = this.copy(model = model.encode)
  def decode: DatasetDetail = this.copy(model = model.decode)
}
