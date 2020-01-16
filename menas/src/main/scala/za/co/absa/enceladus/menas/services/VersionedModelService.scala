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

package za.co.absa.enceladus.menas.services

import org.mongodb.scala.result.UpdateResult
import org.slf4j.LoggerFactory
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import za.co.absa.enceladus.model.UsedIn
import za.co.absa.enceladus.model.menas._
import za.co.absa.enceladus.model.versionedModel.{VersionedModel, VersionedSummary}
import za.co.absa.enceladus.menas.exceptions._
import za.co.absa.enceladus.menas.models.Validation
import za.co.absa.enceladus.menas.repositories.VersionedMongoRepository
import za.co.absa.enceladus.model.menas.audit._

import scala.concurrent.Future

import com.mongodb.MongoWriteException

abstract class VersionedModelService[C <: VersionedModel with Product with Auditable[C]](versionedMongoRepository: VersionedMongoRepository[C])
  extends ModelService(versionedMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[services] val logger = LoggerFactory.getLogger(this.getClass)

  def getLatestVersions(searchQuery: Option[String]): Future[Seq[VersionedSummary]] = {
    versionedMongoRepository.getLatestVersions(searchQuery)
  }

  def getSearchSuggestions(): Future[Seq[String]] = {
    versionedMongoRepository.getDistinctNamesEnabled()
  }

  def getVersion(name: String, version: Int): Future[Option[C]] = {
    versionedMongoRepository.getVersion(name, version)
  }

  def getAllVersions(name: String): Future[Seq[C]] = {
    versionedMongoRepository.getAllVersions(name)
  }

  def getLatestVersion(name: String, includeDisabled: Boolean = false): Future[Option[C]] = {
    versionedMongoRepository.getLatestVersionValue(name, includeDisabled).flatMap({
      case Some(version) => getVersion(name, version)
      case _ => throw NotFoundException()
    })
  }

  def getLatestVersionNumber(name: String): Future[Int] = {
    versionedMongoRepository.getLatestVersionValue(name).flatMap({
      case Some(version) => Future(version)
      case _ => throw NotFoundException()
    })
  }

  def getLatestVersionValue(name: String): Future[Option[Int]] = {
    versionedMongoRepository.getLatestVersionValue(name)
  }

  private[services] def getParents(name: String, fromVersion: Option[Int] = None): Future[Seq[C]] = {
    for {
      versions <- {
        //store all in version ascending order
        val all = versionedMongoRepository.getAllVersions(name, inclDisabled = true).map(_.sortBy(_.version))
        //get those relevant to us
        if (fromVersion.isDefined) {
          all.map(_.filter(_.version <= fromVersion.get))
        } else {
          all
        }
      }
      res <- {
        //see if this was branched from a different entity
        val topParent = if (versions.isEmpty || versions.head.parent.isEmpty) {
          None
        } else {
          versions.head.parent
        }
        if (topParent.isDefined) {
          getParents(topParent.get.name, Some(topParent.get.version))
        } else {
          Future.successful(Seq())
        }
      }
    } yield res ++ versions
  }

  def getAuditTrail(name: String): Future[AuditTrail] = {
    val allParents = getParents(name)

    allParents.flatMap({ parents =>
      val msgs = if(parents.size < 2) Seq() else {
        val pairs = parents.sliding(2)
        pairs.map(p => p.head.getAuditMessages(p(1))).toSeq
      }
      if(parents.isEmpty) {
        this.getLatestVersion(name).map({
          case Some(entity) => AuditTrail(msgs.reverse :+ entity.createdMessage)
          case None => throw NotFoundException()
        })
      } else {
        Future(AuditTrail(msgs.reverse :+ parents.head.createdMessage))
      }
    })
  }

  def getUsedIn(name: String, version: Option[Int]): Future[UsedIn]

  private[menas] def getMenasRef(item: C): MenasReference = {
    MenasReference(Some(versionedMongoRepository.collectionBaseName), item.name, item.version)
  }

  private[menas] def create(item: C, username: String): Future[Option[C]] = {
    for {
      validation <- validate(item)
      _ <- if (validation.isValid()) {
        versionedMongoRepository.create(item, username)
          .recover {
            case e: MongoWriteException =>
              throw ValidationException(Validation().withError("name", s"entity with name already exists: '${item.name}'"))
          }
      } else {
        throw ValidationException(validation)
      }
      detail <- getLatestVersion(item.name)
    } yield detail
  }

  def recreate(username: String, item: C): Future[Option[C]] = {
    for {
      latestVersion <- getLatestVersionNumber(item.name)
      update <- update(username, item.setVersion(latestVersion).asInstanceOf[C])
    } yield update
  }

  def update(username: String, item: C): Future[Option[C]]

  private[services] def updateFuture(username: String, itemName: String, itemVersion: Int)(transform: C => Future[C]): Future[Option[C]] = {
    for {
      versionToUpdate <- getLatestVersion(itemName, includeDisabled = true)
      transformed <- if (versionToUpdate.isEmpty) {
        Future.failed(NotFoundException(s"Version $itemVersion of $itemName not found"))
      } else if (versionToUpdate.get.version != itemVersion) {
        Future.failed(ValidationException(Validation().withError("version", s"Version $itemVersion of $itemName is not the latest version, therefore cannot be edited")))
      }
      else {
        transform(versionToUpdate.get)
      }
      update <- versionedMongoRepository.update(username, transformed)
        .recover {
          case e: MongoWriteException =>
            throw ValidationException(Validation().withError("version", s"entity '$itemName' with this version already exists: ${itemVersion + 1}"))
        }
    } yield Some(update)
  }

  private[services] def update(username: String, itemName: String, itemVersion: Int)(transform: C => C): Future[Option[C]] = {
    this.updateFuture(username, itemName, itemVersion){ item: C =>
      Future {
        transform(item)
      }
    }
  }

  def findRefEqual(refNameCol: String, refVersionCol: String, name: String, version: Option[Int]): Future[Seq[MenasReference]] = {
    versionedMongoRepository.findRefEqual(refNameCol, refVersionCol, name, version)
  }

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

  def isDisabled(name: String): Future[Boolean] = {
    versionedMongoRepository.isDisabled(name)
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
        if (isUnique) {
          validation
        } else {
          validation.withError("name", s"entity with name already exists: '$name'")
        }
      }
    }
  }

  private def hasWhitespace(name: String): Boolean = !name.matches("""\w+""")

}
