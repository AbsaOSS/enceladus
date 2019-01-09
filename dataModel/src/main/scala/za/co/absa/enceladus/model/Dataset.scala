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

package za.co.absa.enceladus.model

import java.time.ZonedDateTime

import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.versionedModel.VersionedModel

case class Dataset(
  name:    String,
  version: Int,
  description: Option[String]= None,
  
  hdfsPath:        String,
  hdfsPublishPath: String,

  schemaName:    String,
  schemaVersion: Int,

  dateCreated: ZonedDateTime = ZonedDateTime.now(),
  userCreated: String        = null,

  lastUpdated: ZonedDateTime = ZonedDateTime.now(),
  userUpdated: String        = null,

  disabled:     Boolean               = false,
  dateDisabled: Option[ZonedDateTime] = None,
  userDisabled: Option[String]        = None,
  conformance:  List[ConformanceRule]) extends VersionedModel {

  override def setVersion(value: Int): Dataset = this.copy(version = value)
  override def setDisabled(disabled: Boolean): VersionedModel = this.copy(disabled = disabled)
  override def setLastUpdated(time: ZonedDateTime): VersionedModel = this.copy(lastUpdated = time)
  override def setUpdatedUser(user: String): VersionedModel = this.copy(userUpdated = user)
  override def setDescription(desc: Option[String]): VersionedModel = this.copy(description = desc)
  override def setDateCreated(time: ZonedDateTime): VersionedModel = this.copy(dateCreated = time)
  override def setUserCreated(user: String): VersionedModel = this.copy(userCreated = user)
  def setSchemaName(newName: String) = this.copy(schemaName = newName)
  def setSchemaVersion(newVersion: Int) = this.copy(schemaVersion = newVersion)
  def setHDFSPath(newPath: String) = this.copy(hdfsPath = newPath)
  def setHDFSPublishPath(newPublishPath: String) = this.copy(hdfsPublishPath = newPublishPath)

  /**
   * @return a dataset with it's mapping conformance rule attributeMappings where the dots are
   *         <MappingConformanceRule.DOT_REPLACEMENT_SYMBOL>
   */
  def encode: Dataset = substituteMappingConformanceRuleCharacter(this, '.', MappingConformanceRule.DOT_REPLACEMENT_SYMBOL)

  /**
   * @return a dataset with it's mapping conformance rule attributeMappings where the
   *         <MappingConformanceRule.DOT_REPLACEMENT_SYMBOL> are dots
   */
  def decode: Dataset = substituteMappingConformanceRuleCharacter(this, MappingConformanceRule.DOT_REPLACEMENT_SYMBOL, '.')

  private def substituteMappingConformanceRuleCharacter(dataset: Dataset, from: Char, to: Char): Dataset = {
    val conformanceRules = dataset.conformance.map {
      case m: MappingConformanceRule => {
        m.copy(attributeMappings = m.attributeMappings.map(key => {
          (key._1.replace(from, to), key._2)
        }))
      }
      case c: ConformanceRule => c
    }

    dataset.copy(conformance = conformanceRules)
  }
}
