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

package za.co.absa.enceladus.menas.repositories

import scala.concurrent.Future

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.Filters
import org.springframework.stereotype.Repository

import za.co.absa.enceladus.menas.exceptions.NotFoundException
import za.co.absa.enceladus.menas.models.LandingPageInformation
import za.co.absa.enceladus.model

@Repository
class LandingPageStatisticsMongoRepository(mongoDb: MongoDatabase)
  extends MongoRepository[LandingPageInformation](mongoDb) {

  private[menas] override def collectionBaseName: String = LandingPageStatisticsMongoRepository.collectionBaseName
  import scala.concurrent.ExecutionContext.Implicits.global

  def updateStatistics(newStats: LandingPageInformation): Future[_] = {
    for {
      cnt <- collection.countDocuments.toFuture
      res <- if(cnt == 0) collection.insertOne(newStats).toFuture 
               else collection.replaceOne(Filters.where("true"), newStats).toFuture
    } yield res
  }

  def get(): Future[LandingPageInformation] = {
    for {
      res <- collection.find.toFuture()
    } yield if(!res.isEmpty) res.head else throw NotFoundException("Landing page statistics not found")
  }
}

object LandingPageStatisticsMongoRepository {
  val collectionBaseName: String = "lading_page_statistics"
  val collectionName: String = s"$collectionBaseName${model.CollectionSuffix}"
}
