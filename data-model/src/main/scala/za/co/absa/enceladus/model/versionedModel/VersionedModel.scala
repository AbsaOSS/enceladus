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

package za.co.absa.enceladus.model.versionedModel

import java.time.ZonedDateTime

import za.co.absa.enceladus.model.Exportable
import za.co.absa.enceladus.model.backend.MenasReference

trait VersionedModel extends Exportable {
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

  val locked: Option[Boolean]
  val dateLocked: Option[ZonedDateTime]
  val userLocked: Option[String]

  val parent: Option[MenasReference]

  def setVersion(value: Int): VersionedModel
  def setDisabled(disabled: Boolean) : VersionedModel
  def setDateDisabled(time: Option[ZonedDateTime]): VersionedModel
  def setUserDisabled(user: Option[String]): VersionedModel
  def setLastUpdated(time: ZonedDateTime) : VersionedModel
  def setUpdatedUser(user: String): VersionedModel
  def setDescription(desc: Option[String]): VersionedModel
  def setDateCreated(time: ZonedDateTime): VersionedModel
  def setUserCreated(user: String): VersionedModel
  def setLocked(locked: Option[Boolean]): VersionedModel
  def setParent(newParent: Option[MenasReference]): VersionedModel
  def setDateLocked(dateLocked: Option[ZonedDateTime]): VersionedModel
  def setUserLocked(userLocked: Option[String]): VersionedModel

  def lockedWithDefault: Boolean = locked.getOrElse(false)

  def exportItem(): String

  def setCreatedInfo(username: String): VersionedModel = {
    setDateCreated(ZonedDateTime.now).setUserCreated(username)
  }

  def setUpdatedInfo(username: String): VersionedModel = {
    setLastUpdated(ZonedDateTime.now)
      .setUpdatedUser(username)
      .setDisabled(false)
      .setDateDisabled(None)
      .setUserDisabled(None)
  }

}
