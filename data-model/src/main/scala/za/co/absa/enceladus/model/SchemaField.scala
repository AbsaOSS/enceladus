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

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import io.swagger.v3.oas.annotations.media.{Schema => AosSchema}

import scala.annotation.meta.field
import scala.beans.BeanProperty

case class SchemaField
(
  @(AosSchema@field)(example = "field1")
  @BeanProperty name: String,
  @(AosSchema@field)(example = "string")
  @BeanProperty `type`: String,
  @(AosSchema@field)(example = "topfieldA")
  @BeanProperty path: String,  // path up to this field

  // These fields are optional when the type of the field is "array".
  elementType: Option[String] = None,
  containsNull: Option[Boolean] = None,

  @BeanProperty nullable: Boolean,
  metadata: Map[String, String],
  children: Seq[SchemaField]
) {
  @(AosSchema@field)(accessMode = AosSchema.AccessMode.READ_ONLY) // will appear on returned entities, not on payloads
  @JsonProperty("absolutePath")
  def getAbsolutePath: String = {
    if(path.isEmpty) name else s"$path.$name"
  }

  @JsonIgnore
  def getAllChildren: Seq[String] = {
    children.flatMap(child => child.getAllChildren :+ child.getAbsolutePath)
  }

  @JsonIgnore
  def getAllChildrenBasePath: Seq[String] = {
    children.flatMap(child => child.getAllChildrenBasePath :+ child.path)
  }
}
