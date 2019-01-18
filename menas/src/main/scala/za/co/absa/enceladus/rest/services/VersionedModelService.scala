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

package za.co.absa.enceladus.rest.services

import org.slf4j.LoggerFactory
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import za.co.absa.enceladus.model.UsedIn
import za.co.absa.enceladus.model.versionedModel.{ VersionedModel, VersionedSummary }
import za.co.absa.enceladus.rest.repositories.VersionedMongoRepository

import scala.concurrent.Future
import java.time.ZonedDateTime
import za.co.absa.enceladus.model.menas._


abstract class VersionedModelService[C <: VersionedModel](versionedMongoRepository: VersionedMongoRepository[C], auditTrailService: AuditTrailService)
  extends ModelService(versionedMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[services] val logger = LoggerFactory.getLogger(this.getClass)

  def getLatestVersions(): Future[Seq[VersionedSummary]] = {
    versionedMongoRepository.getLatestVersions()
  }

  def getVersion(name: String, version: Int): Future[C] = {
    versionedMongoRepository.getVersion(name, version)
  }

  def getAllVersions(name: String): Future[Seq[C]] = {
    versionedMongoRepository.getAllVersions(name)
  }

  def getLatestVersion(name: String): Future[C] = {
    versionedMongoRepository.getLatestVersionValue(name).flatMap(version => getVersion(name, version))
  }

  def getUsedIn(name: String, version: Option[Int]): Future[UsedIn]

  private[rest] def getMenasRef(item: C): MenasReference = MenasReference(Some(versionedMongoRepository.collectionName), item.name, item.version)

  private[services] def getAuditEntry(item: C, entryType: AuditEntryType, username: String, auditMessage: String): AuditEntry = {
    AuditEntry(menasRef = Some(getMenasRef(item)), entryType = entryType, user = username, timeCreated = ZonedDateTime.now(), message = auditMessage)
  }
  
  private[services] def getAuditEntry(itemName: String, itemVersion: Int, entryType: AuditEntryType, username: String, auditMessage: String): AuditEntry = {
    AuditEntry(menasRef = Some(MenasReference(Some(versionedMongoRepository.collectionName), itemName, itemVersion)), 
        entryType = entryType, user = username, timeCreated = ZonedDateTime.now(), message = auditMessage)
  }

  def create(item: C, username: String): Future[C]
  
  private[services] def create(item: C, username: String, auditMessage: String): Future[C] = {
    for {
      unique <- isUniqueName(item.name)
      _ <- if (unique) versionedMongoRepository.create(item, username)
      else throw new Exception(s"Name already exists: ${item.name}")
      audit <- auditTrailService.create(getAuditEntry(item, CreateEntryType, username, auditMessage))
      detail <- getLatestVersion(item.name)
    } yield detail
  }
  
  def update(username: String, item: C): Future[C]

  private[services] def update(username: String, itemName: String, itemVersion: Int, auditMessage: String)(transform: C => ChangedFieldsUpdateTransformResult[C]): Future[C] = {
    for {
      version <- getVersion(itemName, itemVersion)
      transformResult <- Future.successful(transform(version))
      update <- versionedMongoRepository.update(username, transformResult.updatedEntity)
      audit <- {
        val changedFields = transformResult.getUpdatedFields()
        val changedMessage = if(changedFields.isEmpty) "" else s"Changed fields: ${changedFields.mkString(", ")}"
        auditTrailService.create(getAuditEntry(itemName, version.version, UpdateEntryType, username, s"$auditMessage Updated version: ${version.version}. New Version: ${update.version}. $changedMessage"))
      }
    } yield update
  }

  def findRefEqual(refNameCol: String, refVersionCol: String, name: String, version: Option[Int]): Future[Seq[MenasReference]] = versionedMongoRepository.findRefEqual(refNameCol, refVersionCol, name, version)

  def disableVersion(name: String, version: Option[Int]): Future[Object] = {
    val auth = SecurityContextHolder.getContext.getAuthentication
    val principal = auth.getPrincipal.asInstanceOf[UserDetails]

    getUsedIn(name, version).flatMap { usedR =>
      disableVersion(name, version, usedR, principal)
    }
  }

  private def disableVersion(name: String, version: Option[Int], usedIn: UsedIn, principal: UserDetails): Future[Object] = {
    if (usedIn.nonEmpty) {
      Future.successful(usedIn)
    } else {
      versionedMongoRepository.disableVersion(name, version, principal.getUsername)
    }
  }

}
