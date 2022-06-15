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
import org.mongodb.scala.model.{Filters, _}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.rest_api.models.RunSummary
import za.co.absa.enceladus.rest_api.repositories.RunMongoRepository

import scala.concurrent.Future


@Repository
class RunMongoRepositoryV3 @Autowired()(mongoDb: MongoDatabase) extends RunMongoRepository(mongoDb) {

  /**
   * Yields Latest-of-each run summaries (grouped by datasetName, datasetVersion).
   * Optionally filtered by one of `startDate` (>=)|`sparkAppId`(==)|`uniqueId`(==)
   * The result is ordered by datasetName, datasetVersion (both ascending)
   *
   * @param startDate
   * @param sparkAppId
   * @param uniqueId
   * @return
   */
  def getRunSummariesLatestOfEach(datasetName: Option[String] = None,
                                  datasetVersion: Option[Int] = None,
                                  startDate: Option[String] = None,
                                  sparkAppId: Option[String] = None,
                                  uniqueId: Option[String] = None
                                 ): Future[Seq[RunSummary]] = {
    val exclusiveOptsFilter: Option[Bson] = (startDate, sparkAppId, uniqueId) match {
      case (None, None, None) => None
      case (Some(startDate), None, None) => Some(startDateFromFilter(startDate))
      case (None, Some(sparkAppId), None) => Some(sparkIdFilter(sparkAppId))
      case (None, None, Some(uniqueId)) => Some(Filters.eq("uniqueId", uniqueId))
      case _ => throw new IllegalArgumentException("At most 1 filter of [startDate|sparkAppId|uniqueId] is allowed!")
    }

    val datasetFilter: Option[Bson] = datasetNameVersionOptFilter(datasetName, datasetVersion)
    val combinedFilter: Bson = combineOptFilters(exclusiveOptsFilter, datasetFilter)

    val pipeline = Seq(
      filter(combinedFilter),
      sort(descending("runId")), // this results in Accumulator.first to pickup max version
      group(
        // the fields are specifically selected for RunSummary usage
        id = BsonDocument("""{"dataset": "$dataset", "datasetVersion": "$datasetVersion"}"""),
        Accumulators.first("datasetName", "$dataset"),
        Accumulators.first("datasetVersion", "$datasetVersion"),
        Accumulators.first("runId", "$runId"),
        Accumulators.first("status", "$runStatus.status"),
        Accumulators.first("startDateTime", "$startDateTime"),
        Accumulators.first("runUniqueId", "$uniqueId")
      ),
      project(fields(excludeId())), // id composed of dsName+dsVer no longer needed
      sort(ascending("datasetName", "datasetVersion"))
    )

    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }

  def getRunSummaries(datasetName: Option[String] = None,
                      datasetVersion: Option[Int] = None,
                      startDate: Option[String] = None): Future[Seq[RunSummary]] = {

    val dateFilter: Option[Bson] = startDate.map(date => startDateFromFilter(date))
    val datasetFilter: Option[Bson] = datasetNameVersionOptFilter(datasetName, datasetVersion)
    val combinedFilter: Bson = combineOptFilters(dateFilter, datasetFilter)

    val pipeline = Seq(
      filter(combinedFilter),
      summaryProjection,
      sort(ascending("datasetName", "datasetVersion", "runId"))
    )

    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }

  protected def startDateFromFilter(startDate: String): Bson = {
    // todo use LocalDateTime?
    Filters.gte("startDateTime", startDate)
  }

  protected def datasetNameVersionOptFilter(datasetName: Option[String], datasetVersion: Option[Int]): Option[Bson] = {
    (datasetName, datasetVersion) match {
      case (None, None) => None // all entities
      case (Some(datasetName), None) => Some(Filters.eq("dataset", datasetName))
      case (Some(datasetName), Some(datasetVersion)) => Some(Filters.and(
        Filters.eq("dataset", datasetName),
        Filters.eq("datasetVersion", datasetVersion)
      ))
      case _ => throw new IllegalArgumentException("Disallowed dataset name/version combination." +
        "For dataset (name, version) filtering, the only allowed combinations are:" +
        "(None, None), (Some, None) and (Some, Some)")
    }
  }

  protected def combineOptFilters(optFilter1: Option[Bson], optFilter2: Option[Bson]): Bson = (optFilter1, optFilter2) match {
    case (None, None) => BsonDocument() // empty filter
    case (Some(filter1), None) => filter1
    case (None, Some(filter2)) => filter2
    case (Some(filter1), Some(filter2)) => Filters.and(filter1, filter2)
  }

}
