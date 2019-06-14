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

package za.co.absa.enceladus.migrations.migrations.model1

import java.time.ZonedDateTime

case class MappingTable(name: String,
    version: Int = 1,
    description: Option[String] = None,

    hdfsPath: String,

    schemaName: String,
    schemaVersion: Int,

    defaultMappingValue: List[DefaultValue] = List(),

    dateCreated: ZonedDateTime = ZonedDateTime.now(),
    userCreated: String,

    lastUpdated: ZonedDateTime = ZonedDateTime.now(),
    userUpdated: String,

    disabled: Boolean = false,
    dateDisabled: Option[ZonedDateTime] = None,
    userDisabled: Option[String] = None,
    parent: Option[MenasReference] = None) extends VersionedModel

