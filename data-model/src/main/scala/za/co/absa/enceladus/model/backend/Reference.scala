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

package za.co.absa.enceladus.model.backend

import io.swagger.v3.oas.annotations.media.{Schema => AosSchema}

import scala.annotation.meta.field
import scala.beans.BeanProperty

  @AosSchema(implementation = classOf[Reference],
    example = "{" +
      "\"collection\": \"dataset\"," +
      "\"name\": \"Test\"," +
      "\"version\": 4" +
    "}"
  )
case class Reference(
  @(AosSchema@field)(implementation = classOf[String],  example = "collection1")
  @BeanProperty collection: Option[String],
  @(AosSchema@field)(example = "entity1")
  @BeanProperty name: String,
  @(AosSchema@field)(example = "1")
  @BeanProperty version: Int
)
