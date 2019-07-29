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

package za.co.absa.enceladus.menas.factories

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import za.co.absa.enceladus.model.menas.MenasReference
import za.co.absa.enceladus.model.versionedModel.VersionedModel

trait EntityFactory[T <: VersionedModel] {

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss XXXX")
  val dummyZonedDateTime: ZonedDateTime = ZonedDateTime.parse("04-12-2017 16:19:17 +0000", formatter)

  def collectionBaseName: String

  def toParent(entity: T): MenasReference = {
    MenasReference(Some(collectionBaseName), entity.name, entity.version)
  }

}
