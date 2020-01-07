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

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model._
import org.mongodb.scala.{Completed, Document, MongoDatabase, Observable}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.atum.model.{Checkpoint, ControlMeasure, RunStatus}
import za.co.absa.atum.utils.ControlUtils
import za.co.absa.enceladus.menas.models.{RunDatasetNameGroupedSummary, RunDatasetVersionGroupedSummary, RunSummary}
import za.co.absa.enceladus.model
import za.co.absa.enceladus.model.{Run, SplineReference}

import scala.concurrent.Future

object RunMongoRepository {
  val collectionBaseName: String = "run"
  val collectionName: String = s"$collectionBaseName${model.CollectionSuffix}"
}

@Repository
class RunMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends MongoRepository[Run](mongoDb) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[menas] override def collectionBaseName: String = RunMongoRepository.collectionBaseName

  private val summaryProjection: Bson = project(fields(
    computed("datasetName", "$dataset"),
    computed("status", "$runStatus.status"),
    computed("runUniqueId", "$controlMeasure.runUniqueId"),
    include("datasetVersion", "runId", "startDateTime"),
    excludeId()
  ))

  private def getTodaysFilter() = {
    val date = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy"))
    regex("startDateTime", s"^$date")
  }

  private def getTodayRunsCount(filters: List[Bson]): Future[Int] = {
    val pipeline = Seq(
      filter(and((getTodaysFilter :: filters): _*)),
      Aggregates.count("count"))
    collection.aggregate[BsonDocument](pipeline).headOption().map({
      case Some(doc) => doc.getInt32("count").getValue
      case None => 0
    })
  }

  def getTodaysRuns(): Future[Int] = {
    getTodayRunsCount(List())
  }

  def getTodaysSuccessfulRuns(): Future[Int] = {
    getTodayRunsCount(List(Filters.eq("runStatus.status", "allSucceeded")))
  }

  def getTodaysFailedRuns(): Future[Int] = {
    getTodayRunsCount(List(Filters.eq("runStatus.status", "failed")))
  }

  def getTodaysStdSuccessRuns(): Future[Int] = {
    getTodayRunsCount(List(Filters.eq("runStatus.status", "stageSucceeded")))
  }

  def getTodaysRunningRuns(): Future[Int] = {
    getTodayRunsCount(List(Filters.eq("runStatus.status", "running")))
  }

  def getTodaysSuccessWithErrors(): Future[Int] = {
    getTodayRunsCount(List(
      Filters.eq("runStatus.status", "allSucceeded"),
      or(
        and(Filters.exists("controlMeasure.metadata.additionalInfo.std_errors_count"),
          Filters.notEqual("controlMeasure.metadata.additionalInfo.std_errors_count", "0")),
        and(Filters.exists("controlMeasure.metadata.additionalInfo.conform_errors_count"),
          Filters.notEqual("controlMeasure.metadata.additionalInfo.conform_errors_count", "0")))))
  }

  def getAllLatest(): Future[Seq[Run]] = {
    getLatestOfEach()
      .toFuture()
      .map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

  def getByStartDate(startDate: String): Future[Seq[Run]] = {
    getLatestOfEach(Option(startDate))
      .toFuture()
      .map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

  def getAllSummaries(): Future[Seq[RunSummary]] = {
    val pipeline = Seq(
      summaryProjection,
      sort(ascending("datasetName", "datasetVersion", "runId"))
    )
    collection
      .aggregate[RunSummary](pipeline)
      .allowDiskUse(true)
      .toFuture()
  }

  def getSummariesByDatasetName(datasetName: String): Future[Seq[RunSummary]] = {
    val pipeline = Seq(
      filter(
        equal("dataset", datasetName)
      ),
      summaryProjection,
      sort(ascending("datasetVersion", "runId"))
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
      summaryProjection,
      sort(ascending("runId"))
    )
    collection
      .aggregate[RunSummary](pipeline)
      .toFuture()
  }

  private def getLatestOfEach(startDateOpt: Option[String] = None): Observable[BsonDocument] = {
    val pipeline = startDateOpt.map { startDate =>
      filter(regex("startDateTime", s"^$startDate"))
    }.toList :::
      List(
        group(BsonDocument("""{"dataset": "$dataset", "datasetVersion": "$datasetVersion"}"""), max("latestRun", "$runId")),
        group("""$_id.dataset""",
          max("latestVersion", """$_id.datasetVersion"""),
          push("versionRunMap", BsonDocument("""{ "datasetVersion": "$_id.datasetVersion", "latestRun": "$latestRun" }"""))),
        unwind("$versionRunMap"),
        filter(BsonDocument("""{"$expr": {"$eq": ["$latestVersion", "$versionRunMap.datasetVersion"]}}""")),
        lookup(from = collectionName,
          let = Seq(
            Variable("datasetName", "$_id"),
            Variable("latestVersion", "$latestVersion"),
            Variable("latestRunId", "$versionRunMap.latestRun")
          ),
          pipeline = Seq(
            filter(BsonDocument(
              """
                |{ $expr:
                |    { $and:
                |        [
                |            {$eq: ["$dataset", "$$datasetName"]},
                |            {$eq: ["$datasetVersion", "$$latestVersion"]},
                |            {$eq: ["$runId", "$$latestRunId"]}
                |        ]
                |    }
                |}
                |""".stripMargin))
          ),
          as = "latestRun"),
        replaceRoot(BsonDocument("""{$arrayElemAt: ["$latestRun", 0]}""")),
        sort(ascending("dataset"))
      )

    collection
      .aggregate[BsonDocument](pipeline)
  }

  def getRunSummariesPerDatasetName(): Future[Seq[RunDatasetNameGroupedSummary]] = {
    val pipeline = Seq(
      project(fields(
        include("dataset"),
        Document("""{start: {
                   |  $dateFromString: {
                   |    dateString: "$startDateTime",
                   |    format: "%d-%m-%Y %H:%M:%S %z"
                   |  }
                   |}},""".stripMargin),
        Document(
          """{timezone: {
            |  $substrBytes: [
            |    "$startDateTime", 20, 5
            |  ]
            |}}""".stripMargin),
        excludeId()
      )),
      group("$dataset",
        Accumulators.sum("numberOfRuns", 1),
        Accumulators.max("latestStart", "$start"),
        Accumulators.first("timezone", "$timezone")
      ),
      project(fields(
        computed("datasetName", "$_id"),
        include("numberOfRuns"),
        Document("""{latestRunStartDateTime: {
                   |  $dateToString: {
                   |    date: "$latestStart",
                   |    format: "%d-%m-%Y %H:%M:%S %z",
                   |    timezone: "$timezone"
                   |  }
                   |}},""".stripMargin),
        excludeId()
      )),
      sort(ascending("datasetName"))
    )

    collection
      .aggregate[RunDatasetNameGroupedSummary](pipeline)
      .toFuture()
  }

  def getRunSummariesPerDatasetVersion(datasetName: String): Future[Seq[RunDatasetVersionGroupedSummary]] = {
    val pipeline = Seq(
      filter(equal("dataset", datasetName)),
      project(fields(
        include("dataset", "datasetVersion"),
        Document("""{start: {
                   |  $dateFromString: {
                   |    dateString: "$startDateTime",
                   |    format: "%d-%m-%Y %H:%M:%S %z"
                   |  }
                   |}},""".stripMargin),
        Document(
          """{timezone: {
            |  $substrBytes: [
            |    "$startDateTime", 20, 5
            |  ]
            |}}""".stripMargin),
        excludeId()
      )),
      group("$datasetVersion",
        Accumulators.first("datasetName", "$dataset"),
        Accumulators.sum("numberOfRuns", 1),
        Accumulators.max("latestStart", "$start"),
        Accumulators.first("timezone", "$timezone")
      ),
      project(fields(
        computed("datasetVersion", "$_id"),
        include("datasetName", "numberOfRuns"),
        Document("""{latestRunStartDateTime: {
                   |  $dateToString: {
                   |    date: "$latestStart",
                   |    format: "%d-%m-%Y %H:%M:%S %z",
                   |    timezone: "$timezone"
                   |  }
                   |}},""".stripMargin),
        excludeId()
      )),
      sort(descending("datasetVersion"))
    )

    collection
      .aggregate[RunDatasetVersionGroupedSummary](pipeline)
      .toFuture()
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
