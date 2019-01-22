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

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonDocument
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
    val mapFn    = """function() {
                     |    emit(this.dataset, this)
                     |}""".stripMargin
    val reduceFn = """function(key, values) {
                     |    var latestVersion = Math.max.apply(Math, values.map(x => {return x.datasetVersion;}))
                     |    var latestVersionRuns = values.filter(x => x.datasetVersion == latestVersion)
                     |    var latestRunId = Math.max.apply(Math, latestVersionRuns.map(x => {return x.runId;}))
                     |    return latestVersionRuns.filter(x => x.runId == latestRunId)[0]
                     |}""".stripMargin
    val finalizeFn = """function(key, reducedValue) { return reducedValue }"""

    collection.mapReduce[BsonDocument](mapFn, reduceFn)
      .finalizeFunction(finalizeFn)
      .jsMode(true)
      .toFuture()
      .map(_.map(bson => ControlUtils.fromJson[RunWrapper](bson.toJson).value))
  }

}
