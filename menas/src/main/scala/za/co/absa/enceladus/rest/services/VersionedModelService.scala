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

package za.co.absa.enceladus.rest.services

import org.mongodb.scala.result.UpdateResult
import org.slf4j.LoggerFactory
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import za.co.absa.enceladus.model.UsedIn
import za.co.absa.enceladus.model.versionedModel.{ VersionedModel, VersionedSummary }
import za.co.absa.enceladus.rest.repositories.VersionedMongoRepository
import za.co.absa.enceladus.model.UsedIn
import za.co.absa.enceladus.rest.exceptions.{EntityInUseException, ValidationException}
import za.co.absa.enceladus.rest.models.Validation
import scala.concurrent.Future
import java.time.ZonedDateTime
import za.co.absa.enceladus.model.menas._
import za.co.absa.enceladus.rest.exceptions.NotFoundException


abstract class VersionedModelService[C <: VersionedModel](versionedMongoRepository: VersionedMongoRepository[C], auditTrailService: AuditTrailService)
  extends ModelService(versionedMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[services] val logger = LoggerFactory.getLogger(this.getClass)

  def getLatestVersions(): Future[Seq[VersionedSummary]] = {
    versionedMongoRepository.getLatestVersions()
  }

  def getVersion(name: String, version: Int): Future[Option[C]] = {
    versionedMongoRepository.getVersion(name, version)
  }

  def getAllVersions(name: String): Future[Seq[C]] = {
    versionedMongoRepository.getAllVersions(name)
  }

  def getLatestVersion(name: String): Future[Option[C]] = {
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

  def create(item: C, username: String): Future[Option[C]]
  
  private[services] def create(item: C, username: String, auditMessage: String): Future[Option[C]] = {
    for {
      validation <- validate(item)
      _          <-
        if (validation.isValid()) versionedMongoRepository.create(item, username)
        else throw ValidationException(validation)
      audit <- auditTrailService.create(getAuditEntry(item, CreateEntryType, username, auditMessage))
      detail <- getLatestVersion(item.name)
    } yield detail
  }
  
  def update(username: String, item: C): Future[Option[C]]

  private[services] def update(username: String, itemName: String, itemVersion: Int, auditMessage: String)(transform: C => ChangedFieldsUpdateTransformResult[C]): Future[Option[C]] = {
    for {
      version <- getVersion(itemName, itemVersion)
      transformed <- if(version.isEmpty) Future.failed(NotFoundException(s"Version $itemVersion of $itemName not found"))
                      else Future.successful(transform(version.get))
      update <- versionedMongoRepository.update(username, transformed.updatedEntity)
      audit <- {
        val changedFields = transformed.getUpdatedFields()
        val changedMessage = if(changedFields.isEmpty) "" else s"Changed fields: ${changedFields.mkString(", ")}"
        auditTrailService.create(getAuditEntry(itemName, version.get.version, UpdateEntryType, username, s"$auditMessage Updated version: ${version.get.version}. New Version: ${update.version}. $changedMessage"))
      }
    } yield Some(update)
  }

  def findRefEqual(refNameCol: String, refVersionCol: String, name: String, version: Option[Int]): Future[Seq[MenasReference]] = versionedMongoRepository.findRefEqual(refNameCol, refVersionCol, name, version)

  def disableVersion(name: String, version: Option[Int]): Future[UpdateResult] = {
    val auth = SecurityContextHolder.getContext.getAuthentication
    val principal = auth.getPrincipal.asInstanceOf[UserDetails]

    getUsedIn(name, version).flatMap { usedR =>
      disableVersion(name, version, usedR, principal)
    }
  }

  private def disableVersion(name: String, version: Option[Int], usedIn: UsedIn, principal: UserDetails): Future[UpdateResult] = {
    if (usedIn.nonEmpty) {
      throw EntityInUseException(usedIn)
    } else {
      versionedMongoRepository.disableVersion(name, version, principal.getUsername)
    }
  }

  def validate(item: C): Future[Validation] = {
    validateName(item.name)
  }

  protected[services] def validateName(name: String): Future[Validation] = {
    val validation = Validation()

    if (hasWhitespace(name)) {
      Future.successful(validation.withError("name", s"name contains whitespace: '$name'"))
    } else {
      isUniqueName(name).map { isUnique =>
        if (isUnique) validation
        else validation.withError("name", s"entity with name already exists: '$name'")
      }
    }
  }

  private def hasWhitespace(name: String): Boolean = !name.matches("""\w+""")

}
