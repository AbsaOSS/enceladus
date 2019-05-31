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

trait VersionedModel {
  val name: String
  val version: Int
  val description: Option[String]
  
  val dateCreated: ZonedDateTime
  val userCreated: String
  
  val lastUpdated: ZonedDateTime
  val userUpdated: String
  
  val disabled: Boolean
  val dateDisabled: Option[ZonedDateTime]
  val userDisabled: Option[String]
  
  val parent: Option[MenasReference]
}
