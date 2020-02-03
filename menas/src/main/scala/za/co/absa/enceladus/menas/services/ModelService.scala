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

import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.menas.repositories.MongoRepository

import scala.concurrent.Future

abstract class ModelService[C](mongoRepository: MongoRepository[C]) {

  import scala.concurrent.ExecutionContext.Implicits.global

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def isUniqueName(name: String): Future[Boolean] = {
    mongoRepository.isUniqueName(name)
  }

  def create(item: C): Future[C] = {
    mongoRepository.create(item).map(_ => item)
  }

}

