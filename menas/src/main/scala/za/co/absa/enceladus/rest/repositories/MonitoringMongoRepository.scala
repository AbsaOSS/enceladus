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
import org.mongodb.scala._

import scala.collection.immutable.HashMap
//import org.mongodb.scala.{AggregateObservable, Completed, Document, MapReduceObservable, MongoDatabase}
//import org.mongodb.scala.bson.BsonDocument
//import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
//import org.mongodb.scala.model.Sorts._
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

@Repository
class MonitoringMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends MongoRepository[Document](mongoDb) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[repositories] override def collectionName: String = "run"

  def getMonitoringDataPoints(datasetName: String): Future[Seq[String]] = {
    /***val responce: Future[Seq[Run]] = collection
      .aggregate(Seq(
        filter(equal("dataset", datasetName)),
        addFields(),
        sort(orderBy(
          descending("controlMeasure.metadata.informationDate"),
          descending("controlMeasure.metadata.version"),
          descending("startDateTime")
        ))
      )).toFuture() ***/
    //responce.toFuture().map(_.map(bson => ControlUtils.fromJson[MonitoringDataPointWrapper](bson.toJson).value))
    val responce = collection.aggregate(Seq(filter(equal("dataset", datasetName))))
    responce
      //.toFuture()
      //.map(_.map(bson => bson.toJson()))
    //responce.subscribe((result: Document) => ControlUtils.fromJson[Document](result.toJson))
    //ControlUtils.fromJson[Run](bson.toJson)
    //responce.map(bson => ControlUtils.fromJson[Document](bson.toJson)).toFuture()
    //collection.find().first().printHeadResult()
    //responce
    Future(Seq("a","b"))
  }

}
