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
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.essentiality.Essentiality
import za.co.absa.enceladus.model.properties.propertyType.{PropertyType, StringPropertyType}

object PropertyDefinitionFactory extends EntityFactory[PropertyDefinition] {

  override val collectionBaseName: String = "propertydef"


  def getDummyPropertyDefinition(name: String = "dummyName",
                                 version: Int = 1,
                                 description: Option[String] = None,
                                 propertyType: PropertyType = StringPropertyType(),
                                 putIntoInfoFile: Boolean = false,
                                 essentiality: Essentiality = Essentiality.Optional,
                                 disabled: Boolean = false,
                                 dateCreated: ZonedDateTime = dummyZonedDateTime,
                                 userCreated: String = "dummyUser",
                                 lastUpdated: ZonedDateTime = dummyZonedDateTime,
                                 userUpdated: String = "dummyUser",
                                 dateDisabled: Option[ZonedDateTime] = None,
                                 userDisabled: Option[String] = None,
                                 parent: Option[MenasReference] = None): PropertyDefinition = {

    PropertyDefinition(name,
      version,
      description,
      propertyType,
      putIntoInfoFile,
      essentiality,
      disabled,
      dateCreated,
      userCreated,
      lastUpdated,
      userUpdated,
      dateDisabled,
      userDisabled,
      parent)
  }

}
