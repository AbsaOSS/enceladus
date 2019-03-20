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

import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts.descending
import org.mongodb.scala.model._
import org.mongodb.scala.{Completed, MapReduceObservable, MongoDatabase}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.atum.utils.ControlUtils
import za.co.absa.enceladus.model.{Run, SplineReference}
import za.co.absa.enceladus.rest.models.{RunSummary, RunWrapper}

import scala.concurrent.Future

object RunMongoRepository {
  val collectionName = "run"
}

@Repository
class RunMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends MongoRepository[Run](mongoDb) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[rest] override def collectionName: String = RunMongoRepository.collectionName

  private val summaryProjection: Bson = project(fields(
    computed("datasetName", "$dataset"),
    computed("status", "$runStatus.status"),
    include("datasetVersion", "runId", "startDateTime"),
    excludeId()
  ))

  def getAllLatest(): Future[Seq[Run]] = {
    getLatestOfEach()
      .toFuture()
      .map(_.map(bson => ControlUtils.fromJson[RunWrapper](bson.toJson).value))
  }

  def getByStartDate(startDate: String): Future[Seq[Run]] = {
    getLatestOfEach()
      .filter(regex("startDateTime", s"^$startDate"))
      .toFuture()
      .map(_.map(bson => ControlUtils.fromJson[RunWrapper](bson.toJson).value))
  }

  def getAllSummaries(): Future[Seq[RunSummary]] = {
    val pipeline = Seq(
      summaryProjection
    )
    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }

  def getSummariesByDatasetName(datasetName: String): Future[Seq[RunSummary]] = {
    val pipeline = Seq(
      filter(
        equal("dataset", datasetName)
      ),
      summaryProjection
    )
    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }
  def getSummariesByDatasetNameAndVersion(datasetName: String, datasetVersion: Int): Future[Seq[RunSummary]] = {
    val pipeline = Seq(
      filter(and(
        equal("dataset", datasetName),
        equal("datasetVersion", datasetVersion)
      )),
      summaryProjection
    )
    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }

  private def getLatestOfEach(): MapReduceObservable[BsonDocument] = {
    val mapFn =
      """function() {
        |  emit(this.dataset, this)
        |}""".stripMargin
    val reduceFn =
      """function(key, values) {
        |  var latestVersion = Math.max.apply(Math, values.map(x => {return x.datasetVersion;}))
        |  var latestVersionRuns = values.filter(x => x.datasetVersion == latestVersion)
        |  var latestRunId = Math.max.apply(Math, latestVersionRuns.map(x => {return x.runId;}))
        |  return latestVersionRuns.filter(x => x.runId == latestRunId)[0]
        |}""".stripMargin
    val finalizeFn =
      """function(key, reducedValue) {
        |  return reducedValue
        |}""".stripMargin

    collection
      .mapReduce[BsonDocument](mapFn, reduceFn)
      .finalizeFunction(finalizeFn)
      .jsMode(true)
  }

  def getRun(datasetName: String, datasetVersion: Int, runId: Int): Future[Option[Run]] = {
    val datasetFilter = getDatasetFilter(datasetName, datasetVersion)
    val runIdEqFilter = equal("runId", runId)

    collection
      .find[BsonDocument](and(datasetFilter, runIdEqFilter))
      .headOption()
      .map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

  def getLatestRun(datasetName: String, datasetVersion: Int): Future[Option[Run]] = {
    val datasetFilter = getDatasetFilter(datasetName, datasetVersion)

    collection
      .find[BsonDocument](datasetFilter)
      .sort(descending("runId"))
      .headOption()
      .map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

  override def create(item: Run): Future[Completed] = {
    val bson = BsonDocument(ControlUtils.asJson(item))
    collection.withDocumentClass[BsonDocument].insertOne(bson).head()
  }

  def appendCheckpoint(uniqueId: String, checkpoint: Checkpoint): Future[Option[Run]] = {
    val bsonCheckpoint = BsonDocument(ControlUtils.asJson(checkpoint))
    collection.withDocumentClass[BsonDocument].findOneAndUpdate(
      equal("uniqueId", uniqueId),
      Updates.addToSet("controlMeasure.checkpoints", bsonCheckpoint),
      FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
    ).headOption().map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

  def updateControlMeasure(uniqueId: String, controlMeasure: ControlMeasure): Future[Option[Run]] = {
    val bsonControlMeasure = BsonDocument(ControlUtils.asJson(controlMeasure))
    collection.withDocumentClass[BsonDocument].findOneAndUpdate(
      equal("uniqueId", uniqueId),
      Updates.set("controlMeasure", bsonControlMeasure),
      FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
    ).headOption().map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

  def updateSplineReference(uniqueId: String, splineRef: SplineReference): Future[Option[Run]] = {
    val bsonSplineRef = BsonDocument(ControlUtils.asJson(splineRef))
    collection.withDocumentClass[BsonDocument].findOneAndUpdate(
      equal("uniqueId", uniqueId),
      Updates.set("splineRef", bsonSplineRef),
      FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
    ).headOption().map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

  def updateRunStatus(uniqueId: String, runStatus: RunStatus): Future[Option[Run]] = {
    val bsonRunStatus = BsonDocument(ControlUtils.asJson(runStatus))
    collection.withDocumentClass[BsonDocument].findOneAndUpdate(
      equal("uniqueId", uniqueId),
      Updates.set("runStatus", bsonRunStatus),
      FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
    ).headOption().map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

  def existsId(uniqueId: String): Future[Boolean] = {
    collection.countDocuments(equal("uniqueId", uniqueId))
      .map(_ > 0).head()
  }

  private def getDatasetFilter(datasetName: String, datasetVersion: Int): Bson = {
    val datasetNameEq = equal("dataset", datasetName)
    val datasetVersionEq = equal("datasetVersion", datasetVersion)

    and(datasetNameEq, datasetVersionEq)
  }

}
