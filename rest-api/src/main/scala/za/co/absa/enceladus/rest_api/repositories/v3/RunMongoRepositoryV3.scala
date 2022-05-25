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

package za.co.absa.enceladus.rest_api.repositories.v3

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.rest_api.models.RunSummary
import za.co.absa.enceladus.rest_api.repositories.RunMongoRepository

import scala.concurrent.Future


@Repository
class RunMongoRepositoryV3 @Autowired()(mongoDb: MongoDatabase) extends RunMongoRepository(mongoDb) {

  /**
   * Grouped by datasetName, datasetVersion
   */

  /**
   * Yields Latest-of-each run summaries (grouped by datasetName, datasetVersion).
   * Optionally filtered by one of `startDate` (>=)|`sparkAppId`(==)|`uniqueId`(==)
   * The result is ordered by datasetName, datasetVersion (both ascending)
   * @param startDate
   * @param sparkAppId
   * @param uniqueId
   * @return
   */
  def getLatestOfEachRunSummary(startDate: Option[String] = None,
                                sparkAppId: Option[String] = None,
                                uniqueId: Option[String] = None
                               ): Future[Seq[RunSummary]] = {
    require(Seq(startDate, sparkAppId, uniqueId).filter(_.isDefined).length <= 1,
      "You may only supply one of startDate|sparkAppId|uniqueId")
    val prefilter = (startDate, sparkAppId, uniqueId) match {
      case (None, None, None) => BsonDocument() // empty filter
      case (Some(startDate), None, None) => startDateFromFilter(startDate)
      case (None, Some(sparkAppId), None) => sparkIdFilter(sparkAppId)
      case (None, None, Some(uniqueId)) => Filters.eq("uniqueId", uniqueId)
      case _ => throw new IllegalArgumentException("At most 1 filter of [startDate|sparkAppId|uniqueId] is allowed!")
    }

    val pipeline = Seq(
        filter(prefilter),
        sort(descending("runId")), // this results in Accumulator.first to pickup max version
        group(
          // the fields are specifically selected for RunSummary usage
          id =  BsonDocument("""{"dataset": "$dataset", "datasetVersion": "$datasetVersion"}"""),
          Accumulators.first("datasetName", "$dataset"),
          Accumulators.first("datasetVersion", "$datasetVersion"),
          Accumulators.first("runId", "$runId" ),
          Accumulators.first("status", "$runStatus.status" ),
          Accumulators.first("startDateTime", "$startDateTime"),
          Accumulators.first("runUniqueId", "$uniqueId" )
        ),
        project(fields(excludeId())), // id composed of dsName+dsVer no longer needed
        sort(ascending("datasetName", "datasetVersion"))
      )

    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }

  protected def startDateFromFilter(startDate: String): Bson = {
    // todo use LocalDateTime?
    Filters.gte("startDateTime", startDate)
  }
}
