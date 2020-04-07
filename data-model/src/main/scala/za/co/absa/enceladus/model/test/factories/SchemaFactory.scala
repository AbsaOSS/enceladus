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

package za.co.absa.enceladus.model.test.factories

import java.time.ZonedDateTime

import za.co.absa.enceladus.model.menas.MenasReference
import za.co.absa.enceladus.model.{Schema, SchemaField}

object SchemaFactory extends EntityFactory[Schema] {

  override val collectionBaseName: String = "schema"

  def getDummySchema(name: String = "dummyName",
                     version: Int = 1,
                     description: Option[String] = None,
                     dateCreated: ZonedDateTime = dummyZonedDateTime,
                     userCreated: String = "dummyUser",
                     lastUpdated: ZonedDateTime = dummyZonedDateTime,
                     userUpdated: String = "dummyUser",
                     disabled: Boolean = false,
                     dateDisabled: Option[ZonedDateTime] = None,
                     userDisabled: Option[String] = None,
                     fields: List[SchemaField] = List(),
                     parent: Option[MenasReference] = None): Schema = {

    Schema(name,
      version,
      description,
      dateCreated,
      userCreated,
      lastUpdated,
      userUpdated,
      disabled,
      dateDisabled,
      userDisabled,
      fields,
      parent)
  }

  def getDummySchemaField(name: String = "dummyFieldName",
                          fieldType: String = "string",
                          path: String = "dummy.path",
                          elementType: Option[String] = None,
                          containsNull: Option[Boolean] = None,
                          nullable: Boolean = true,
                          metadata: Map[String, String] = Map(),
                          children: Seq[SchemaField] = Nil): SchemaField = {
    SchemaField(
      name,
      fieldType,
      path,
      elementType,
      containsNull,
      nullable,
      metadata,
      children
    )
  }

}
