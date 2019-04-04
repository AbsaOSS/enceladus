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

package za.co.absa.enceladus.rest.repositories
import java.util

import org.json4s.jackson.Json
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.Field

import scala.collection.immutable.HashMap
//import org.mongodb.scala.{AggregateObservable, Completed, Document, MapReduceObservable, MongoDatabase}
//import org.mongodb.scala.bson.BsonDocument
//import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
//import org.mongodb.scala.model.{FindOneAndUpdateOptions, ReturnDocument, Updates}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.atum.utils.ControlUtils
import za.co.absa.enceladus.model.{Run, SplineReference}
import za.co.absa.enceladus.rest.models.{MonitoringDataPoint, MonitoringDataPointWrapper}
import za.co.absa.enceladus.rest.models.RunWrapper
import scala.concurrent.Future
import org.mongodb.scala.Document

object MonitoringMongoRepository {
  val collectionName = "run"
}

@Repository
class MonitoringMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends MongoRepository[Run](mongoDb) {

  import scala.concurrent.ExecutionContext.Implicits.global
  private[repositories] override def collectionName: String = MonitoringMongoRepository.collectionName


  def getMonitoringDataPoints(datasetName: String): Future[Seq[String]] = {
    // scala mongodb driver does not yet support all mql features, so we use Document()
    val observable: AggregateObservable[Document] = collection
      .aggregate(Seq(
        // filter by dataset name
        filter(equal("dataset", datasetName)),
        //add casted dates
        Document("""{$addFields: {
                   |          startDateTimeCasted: {
                   |              $dateFromString: {
                   |                  dateString: "$startDateTime",
                   |                  format: "%d-%m-%Y %H:%M:%S %z",
                   |                  onError: "$startDateTime"
                   |              }
                   |          },
                   |          informationDateCasted: {
                   |              $dateFromString: {
                   |                  dateString: "$controlMeasure.metadata.informationDate",
                   |                  format: "%d-%m-%Y",
                   |                  onError: "wrongFormat"
                   |              }
                   |          },
                   | }},""".stripMargin),
        // TODO: filter by informationDateCasted in order to limit the number of elements
        // bring the raw checkpoint to root for further access
        Document(""" {$addFields: {
                   |           raw_checkpoint : {
                   |              $arrayElemAt : [ {
                   |                  $filter : {
                   |                      input : "$controlMeasure.checkpoints",
                   |                      as : "checkpoint",
                   |                      cond : { $eq : [ "$$checkpoint.name", "Raw"] }
                   |                  }
                   |              }, 0 ]
                   |          }
                   |      }}""".stripMargin),
        // add the raw record count
        Document(""" {$addFields: {
                   |           raw_recordcount_control : {
                   |              $arrayElemAt : [ {
                   |                  $filter : {
                   |                      input : "$raw_checkpoint.controls",
                   |                      as : "control",
                   |                      cond : { $eq : [ "$$control.controlName", "recordcount"] }
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
                   |
                   |      }}""".stripMargin),
        // sort the final results
        Document("""{$sort: {informationDateCasted : -1, reportVersion: -1}}""".stripMargin)

        //addFields(new Field("startDateTimeCasted", "blah"))
        /***project( Document("""{startDateTimeCasted: {$dateFromString: {
                                                                          dateString: "$startDateTime",
                                                                          format: "%d-%m-%Y %H:%M:%S %z",
                                                                          onError: "$startDateTime"
                                                                      }}}""")) ***/
       // sort(orderBy(
       //   descending("controlMeasure.metadata.informationDate"),
       //   descending("controlMeasure.metadata.version"),
       //   descending("startDateTime")
       // ))
      ))

    //val observable: AggregateObservable[Document] = collection.aggregate(Seq(filter(equal("dataset", datasetName))))

    observable.map(doc => doc.toJson).toFuture()
  }

}

