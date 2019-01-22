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
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.{MapReduceObservable, MongoDatabase}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.atum.utils.ControlUtils
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.rest.models.RunWrapper

import scala.concurrent.Future

@Repository
class RunMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends MongoRepository[Run](mongoDb) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override private[repositories] def collectionName = "run"

  def getAllLatest(): Future[Seq[Run]] = {
    getLatestOfEach()
      .toFuture()
      .map(_.map(bson => ControlUtils.fromJson[RunWrapper](bson.toJson).value))
  }

  def getByStartDate(startDate: String): Future[Seq[Run]] = {
    getLatestOfEach()
      .filter(regex("startDateTime", s"^$startDate\\s+"))
      .toFuture()
      .map(_.map(bson => ControlUtils.fromJson[RunWrapper](bson.toJson).value))
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
    val datasetNameEq = equal("dataset", datasetName)
    val datasetVersionEq = equal("datasetVersion", datasetVersion)
    val runIdEq = equal("runId", runId)

    collection
      .find[BsonDocument](and(datasetNameEq, datasetVersionEq, runIdEq))
      .headOption()
      .map(_.map(bson => ControlUtils.fromJson[Run](bson.toJson)))
  }

}
