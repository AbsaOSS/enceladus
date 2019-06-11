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
import org.mongodb.scala.model.Aggregates.filter
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.Document
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.model.Run
import scala.concurrent.Future

object MonitoringMongoRepository {
  val collectionName = "run"
}

@Repository
class MonitoringMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends MongoRepository[Run](mongoDb) {
  private[menas] override def collectionName: String = MonitoringMongoRepository.collectionName


  def getMonitoringDataPoints(datasetName: String, startDate: String, endDate: String): Future[Seq[String]] = {
    // scala mongodb driver does not yet support all mql features, so we use Document() with aggregate pipelines
    val observable: AggregateObservable[Document] = collection
      .aggregate(Seq(
        // filter by dataset name
        filter(equal("dataset", datasetName)),
        //add casted dates
        Document("""{$addFields: {
                   |          startDateTimeCasted: {
                   |              $dateFromString: {
                   |                  dateString: "$startDateTime",
                   |                  onError: "wrongFormat"
                   |              }
                   |          },
                   |          informationDateCasted: {
                   |              $dateFromString: {
                   |                  dateString: "$controlMeasure.metadata.informationDate",
                   |                  onError: "wrongFormat"
                   |              }
                   |          }
                   | }},""".stripMargin),
        // filter by informationDateCasted in order to limit the number of elements
        Document(
          s"""
            |{ $$match: {
            |    informationDateCasted: {$$gte: ISODate("${startDate}T00:00:00.0Z"), $$lte: ISODate("${endDate}T00:00:00.0Z") }
            |}},""".stripMargin),
        // bring the raw checkpoint to root for further access
        Document(""" {$addFields: {
                   |           raw_checkpoint : {
                   |              $arrayElemAt: ["$controlMeasure.checkpoints", 0]
                   |          }
                   |      }}""".stripMargin),
        // add the raw recordcount
        Document(""" {$addFields: {
                   |           raw_recordcount_control : {
                   |              $arrayElemAt : [ {
                   |                  $filter : {
                   |                      input : "$raw_checkpoint.controls",
                   |                      as : "control",
                   |                      cond : { $or: [
                   |                        {$eq : [ "$$control.controlName", "recordCount"]},
                   |                        {$eq : [ "$$control.controlName", "recordcount"]}
                   |                      ]}
                   |                  }
                   |              }, 0 ]
                   |          }
                   |      }}""".stripMargin),
        // sort intermidiate results before further grouping (needed as we use $first to keep the latest run only)
        Document(
          """{$sort: {
            |    informationDateCasted : -1,
            |    "controlMeasure.metadata.version" : -1,
            |    startDateTimeCasted : -1
            |} }""".stripMargin),
        // group, so that we have a single object per infoDate and report version, which corresponds to the latest run
        // here we also project the data of interest
        Document(""" {$group : {
                   |          "_id": {informationDateCasted: "$informationDateCasted", reportVersion: "$controlMeasure.metadata.version"},
                   |          "datasetName": {$first: "$dataset"},
                   |          "runObjectId" : {$first: "$_id"},
                   |          "startDateTime" : {$first: "$startDateTimeCasted"},
                   |          "datasetVersion" : {$first: "$datasetVersion"},
                   |          informationDate : {$first: "$controlMeasure.metadata.informationDate"},
                   |          informationDateCasted : {$first: "$informationDateCasted"},
                   |          reportVersion : {$first: "$controlMeasure.metadata.version"},
                   |          "runId" : {$first: "$runId"} ,
                   |          "status" : {$first: "$runStatus.status"},
                   |          "std_records_succeeded" : {$first: "$controlMeasure.metadata.additionalInfo.std_records_succeeded"},
                   |          "std_records_failed" : {$first: "$controlMeasure.metadata.additionalInfo.std_records_failed"},
                   |          "conform_records_succeeded" : {$first: "$controlMeasure.metadata.additionalInfo.conform_records_succeeded"},
                   |          "conform_records_failed" : {$first: "$controlMeasure.metadata.additionalInfo.conform_records_failed"},
                   |          raw_recordcount : {$first: "$raw_recordcount_control.controlValue"},
                   |          latestCheckpoint: {$first: {$slice: ["$controlMeasure.checkpoints", -1]}},
                   |          "publish_dir_size" : {$first: "$controlMeasure.metadata.additionalInfo.publish_dir_size"},
                   |          "std_dir_size" : {$first: "$controlMeasure.metadata.additionalInfo.std_dir_size"},
                   |          "raw_dir_size" : {$first: "$controlMeasure.metadata.additionalInfo.raw_dir_size"}
                   |
                   |      }}""".stripMargin),
        // sort the final results
        Document("""{$sort: {informationDateCasted : -1, reportVersion: -1}}""".stripMargin)
      ))
    observable.map(doc => doc.toJson).toFuture()
  }
}

