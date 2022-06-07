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

package za.co.absa.enceladus.rest_api.services.v3

import org.mongodb.scala.result.UpdateResult
import org.slf4j.LoggerFactory
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import za.co.absa.enceladus.model.{UsedIn, Validation}
import za.co.absa.enceladus.model.menas.audit._
import za.co.absa.enceladus.model.versionedModel.{VersionedModel, VersionedSummary}
import za.co.absa.enceladus.rest_api.exceptions._
import za.co.absa.enceladus.rest_api.services.VersionedModelService

import scala.concurrent.Future

// scalastyle:off number.of.methods
trait VersionedModelServiceV3[C <: VersionedModel with Product with Auditable[C]] extends VersionedModelService[C] {

  import scala.concurrent.ExecutionContext.Implicits.global

  override private[services] val logger = LoggerFactory.getLogger(this.getClass)

  def getLatestVersionSummary(name: String): Future[Option[VersionedSummary]] = {
    mongoRepository.getLatestVersionSummary(name)
  }

  // v3 has internal validation on importItem (because it is based on update), v2 only had it on import
  override def importSingleItem(item: C, username: String, metadata: Map[String, String]): Future[Option[(C, Validation)]] = {
    val metadataValidation = validateMetadata(metadata)
    if (metadataValidation.isValid) {
      // even if valid, merge validations results (warnings may be theoretically present)
      importItem(item, username).map(_.map { case (item, validation) => (item, validation.merge(metadataValidation)) })
    } else {
      Future.failed(ValidationException(metadataValidation))
    }
  }

  /**
   * Enables all versions of the entity by name.
   * @param name
   */
  def enableEntity(name: String): Future[UpdateResult] = {
    val auth = SecurityContextHolder.getContext.getAuthentication
    val principal = auth.getPrincipal.asInstanceOf[UserDetails]

    // todo check validation of used-in dependees? #2065

    mongoRepository.enableAllVersions(name, principal.getUsername)
  }


  /**
   * Retrieves model@version and calls
   * [[validate(java.lang.Object)]]
   *
   * In order to extend this behavior, override the mentioned method instead. (that's why this is `final`)
   */
  final def validate(name: String, version: Int): Future[Validation] = {
    getVersion(name, version).flatMap({
      case Some(entity) => validate(entity)
      case _ => Future.failed(NotFoundException(s"Entity by name=$name, version=$version is not found!"))
    })
  }

  /**
   * Validates the item as V2 + checks that it is enabled
   */
  override def validate(item: C): Future[Validation] = {
    for {
      superValidation <- super.validate(item)
      enabledValidation <- validateEnabled(item)
    } yield superValidation.merge(enabledValidation)
  }

  protected[services] def validateEnabled(item: C): Future[Validation] = {
    if (item.disabled) {
      Future.successful(Validation.empty.withError("disabled", s"Entity ${item.name} is disabled!"))
    } else {
      Future.successful(Validation.empty)
    }
  }

}


