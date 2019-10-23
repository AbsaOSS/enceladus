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

package za.co.absa.enceladus.menas.repositories

import org.mongodb.scala.{AggregateObservable, MongoDatabase}
import org.mongodb.scala.model.Aggregates.{filter, group, limit, sort}
import org.mongodb.scala.model.Accumulators.{first, sum}
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Sorts.{descending, orderBy}
import org.mongodb.scala.Document
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.model.Run

import scala.concurrent.Future

object MonitoringMongoRepository {
  val collectionBaseName: String = "run"
}

@Repository
class MonitoringMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends MongoRepository[Run](mongoDb) {

  private[menas] override def collectionBaseName: String = MonitoringMongoRepository.collectionBaseName

  @Value("${za.co.absa.enceladus.menas.monitoring.fetch.limit}")
  private val fetchLimit: Integer = null // scalastyle:ignore null


  def getMonitoringDataPoints(datasetName: String, startDate: String, endDate: String): Future[Seq[String]] = {
    // scala mongodb driver does not yet support all mql features, working with dates in particular,
    // therefore, we use Document() in corresponding aggregate pipelines
    val observable: AggregateObservable[Document] = collection
      .aggregate(List(
        // filter by dataset name
        filter(equal("dataset", datasetName)),
        //add casted dates
        Document("""{$addFields: {
                   |  startDateTimeCasted: {
                   |    $dateFromString: {
                   |      dateString: "$startDateTime",
                   |      onError: "wrongFormat"
                   |    }
                   |  },
                   |  informationDateCasted: {
                   |    $dateFromString: {
                   |      dateString: "$controlMeasure.metadata.informationDate",
                   |      onError: "wrongFormat"
                   |    }
                   |  }
                   |}},""".stripMargin),
        // filter by informationDateCasted
        Document(
          s"""{ $$match: {
             |    informationDateCasted: {$$gte: ISODate("${startDate}T00:00:00.0Z"),
             |      $$lte: ISODate("${endDate}T00:00:00.0Z") }
             |}},""".stripMargin),
        // sort intermidiate results before further grouping (needed as we use $first to keep the latest run only)
        sort(orderBy(
          descending("informationDateCasted"),
          descending("controlMeasure.metadata.version"),
          descending("startDateTimeCasted"))),
        // group, so that we have a single object per infoDate and report version, which corresponds to the latest run
        // here we also project the data of interest
        group(
          Document("""{informationDateCasted: "$informationDateCasted",
            | reportVersion: "$controlMeasure.metadata.version"}""".stripMargin),
          first("datasetName", "$dataset" ),
          first("runObjectId", "$_id"),
          first("startDateTimeCasted", "$startDateTimeCasted"),
          first("datasetVersion", "$datasetVersion"),
          first("informationDate", "$controlMeasure.metadata.informationDate"),
          first("informationDateCasted", "$informationDateCasted"),
          first("reportVersion", "$controlMeasure.metadata.version"),
          first("runId", "$runId"),
          first("status", "$runStatus.status"),
          first("std_records_succeeded", "$controlMeasure.metadata.additionalInfo.std_records_succeeded"),
          first("std_records_failed", "$controlMeasure.metadata.additionalInfo.std_records_failed"),
          first("conform_records_succeeded", "$controlMeasure.metadata.additionalInfo.conform_records_succeeded"),
          first("conform_records_failed", "$controlMeasure.metadata.additionalInfo.conform_records_failed"),
          first("controlMeasure", "$controlMeasure"),
          sum("runAttempts", 1)
        ),
        // sort the final results
        sort(orderBy(
          descending("informationDateCasted"),
          descending("reportVersion")
        )),
        limit(this.fetchLimit)
      ))
    observable.map(doc => doc.toJson).toFuture()
  }
}

