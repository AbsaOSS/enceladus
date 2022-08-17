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

import za.co.absa.enceladus.model.dataFrameFilter.DataFrameFilter
import za.co.absa.enceladus.model.backend.MenasReference
import za.co.absa.enceladus.model.{DefaultValue, MappingTable, Schema}

object MappingTableFactory extends EntityFactory[Schema] {

  override val collectionBaseName: String = "mapping_table"

  def getDummyMappingTable(name: String = "dummyName",
                           version: Int = 1,
                           description: Option[String] = None,
                           hdfsPath: String = "/dummy/path",
                           schemaName: String = "dummySchema",
                           schemaVersion: Int = 1,
                           defaultMappingValue: List[DefaultValue] = List(),
                           dateCreated: ZonedDateTime = dummyZonedDateTime,
                           userCreated: String = "dummyUser",
                           lastUpdated: ZonedDateTime = dummyZonedDateTime,
                           userUpdated: String = "dummyUser",
                           disabled: Boolean = false,
                           dateDisabled: Option[ZonedDateTime] = None,
                           userDisabled: Option[String] = None,
                           locked: Option[Boolean] = None,
                           dateLocked: Option[ZonedDateTime] = None,
                           userLocked: Option[String] = None,
                           parent: Option[MenasReference] = None,
                           filter: Option[DataFrameFilter] = None): MappingTable = {

    MappingTable(name,
      version,
      description,
      hdfsPath,
      schemaName,
      schemaVersion,
      defaultMappingValue,
      dateCreated,
      userCreated,
      lastUpdated,
      userUpdated,
      disabled,
      dateDisabled,
      userDisabled,
      locked,
      dateLocked,
      userLocked,
      parent,
      filter
    )
  }

  def getDummyDefaultValue(columnName: String = "dummyColumnName",
                           value: String = "dummyValue"): DefaultValue = {
    DefaultValue(columnName, value)
  }

}
