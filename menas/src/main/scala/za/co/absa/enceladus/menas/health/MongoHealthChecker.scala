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

package za.co.absa.enceladus.menas.health

import java.util.concurrent.TimeUnit

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.collection.immutable.Document
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.health.{Health, HealthIndicator}
import org.springframework.stereotype.Component

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

@Component("MongoDBConnection")
class MongoHealthChecker @Autowired()(mongoDb: MongoDatabase) extends HealthIndicator {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val ping = Document("{ ping : 1 }")

  override protected def health(): Health = {
    Try {
      Await.ready(mongoDb.runCommand(ping).toFuture(), Duration(1, TimeUnit.SECONDS))
    } match {
      case Success(_) =>
        Health.up().build()
      case Failure(e) =>
        log.error("MongoDB connection is down", e)
        Health.down().withException(e).build()
    }
  }

}
