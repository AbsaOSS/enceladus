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

import org.mongodb.scala.{Document, MongoDatabase}
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
import za.co.absa.enceladus.rest_api.repositories.v3.RunMongoRepositoryV3.emptyBsonFilter

import java.time.LocalDate
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
                                  startDate: Option[LocalDate] = None,
                                  sparkAppId: Option[String] = None,
                                  uniqueId: Option[String] = None,
                                  offset: Option[Int] = None,
                                  limit: Option[Int] = None
                                 ): Future[Seq[RunSummary]] = {
    val exclusiveFilterStage: Seq[Bson] = (startDate, sparkAppId, uniqueId) match {
      case (None, None, None) => Seq()
      case (Some(startDate), None, None) => startDateFilterAggStages(startDate)
      case (None, Some(sparkAppId), None) => Seq(filter(sparkIdFilter(sparkAppId)))
      case (None, None, Some(uniqueId)) => Seq(filter(Filters.eq("uniqueId", uniqueId)))
      case _ => throw new IllegalArgumentException("At most 1 filter of [startDate|sparkAppId|uniqueId] is allowed!")
    }

    val datasetFilter: Bson = datasetNameVersionFilter(datasetName, datasetVersion)

    val pipeline =
      Seq(filter(datasetFilter)) ++
        exclusiveFilterStage ++ // may be empty
        Seq(
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
        ) ++
        offset.map(skipVal => Seq(Aggregates.skip(skipVal))).getOrElse(Seq.empty) ++ // todo reconsider using skip for performance?
        limit.map(limitVal => Seq(Aggregates.limit(limitVal))).getOrElse(Seq.empty)

    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }

  def getRunSummaries(datasetName: Option[String] = None,
                      datasetVersion: Option[Int] = None,
                      startDate: Option[LocalDate] = None,
                      offset: Option[Int] = None,
                      limit: Option[Int] = None): Future[Seq[RunSummary]] = {

    val dateFilterStages: Seq[Bson] = startDate.map(startDateFilterAggStages).getOrElse(Seq.empty)
    val datasetFilter: Bson = datasetNameVersionFilter(datasetName, datasetVersion)

    val pipeline =
      Seq(filter(datasetFilter)) ++
        dateFilterStages ++ // may be empty
        Seq(
          summaryProjection,
          sort(ascending("datasetName", "datasetVersion", "runId"))
        ) ++
        offset.map(skipVal => Seq(Aggregates.skip(skipVal))).getOrElse(Seq.empty) ++
        limit.map(limitVal => Seq(Aggregates.limit(limitVal))).getOrElse(Seq.empty)

    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }

  /**
   * Adds aggregation stages to create a typed version of `startDateTimeTyped` and filters on it to be >= `startDate`
   *
   * @param startDate
   * @return
   */
  protected def startDateFilterAggStages(startDate: LocalDate): Seq[Bson] = {
    Seq(
      addFields(Field("startDateTimeTyped",
        Document(
          """{$dateFromString: {
            |  dateString: "$startDateTime",
            |  format: "%d-%m-%Y %H:%M:%S %z"
            |}}""".stripMargin)
      )),
      filter(Filters.gte("startDateTimeTyped", startDate.atStartOfDay()))
    )
  }

  protected def datasetNameVersionFilter(datasetName: Option[String], datasetVersion: Option[Int]): Bson = {
    (datasetName, datasetVersion) match {
      case (None, None) => emptyBsonFilter // all entities
      case (Some(datasetName), None) => Filters.eq("dataset", datasetName)
      case (Some(datasetName), Some(datasetVersion)) => Filters.and(
        Filters.eq("dataset", datasetName),
        Filters.eq("datasetVersion", datasetVersion)
      )
      case _ => throw new IllegalArgumentException("Disallowed dataset name/version combination." +
        "For dataset (name, version) filtering, the only allowed combinations are:" +
        "(None, None), (Some, None) and (Some, Some)")
    }
  }

}

object RunMongoRepositoryV3 {
  val emptyBsonFilter = BsonDocument()

  def combineFilters(filter1: Bson, filter2: Bson): Bson = (filter1, filter2) match {
    case (doc1, doc2) if doc1 == emptyBsonFilter && doc2 == emptyBsonFilter => emptyBsonFilter
    case (_, doc2) if doc2 == emptyBsonFilter => filter1
    case (doc1, _) if doc1 == emptyBsonFilter => filter2
    case (doc1, doc2) => Filters.and(doc1, doc2)

  }
}
