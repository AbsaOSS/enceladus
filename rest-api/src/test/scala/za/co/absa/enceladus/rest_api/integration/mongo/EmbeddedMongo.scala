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

package za.co.absa.enceladus.rest_api.integration.mongo

import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodStarter}
import de.flapdoodle.embed.process.runtime.Network
import javax.annotation.{PostConstruct, PreDestroy}
import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.{Bean, Configuration, Primary, Profile}
import za.co.absa.enceladus.rest_api.utils.implicits.codecRegistry

/**
 * Provides an embedded local mongo. Spawn it before tests and shutdown after
 */
@Configuration
@Profile(Array("withEmbeddedMongo"))
class EmbeddedMongo {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var mongodExecutable: MongodExecutable = _
  private var mongoPort: Int = _

  def getMongoUri: String = s"mongodb://localhost:$mongoPort/?ssl=false"

  def getMongoPort: Int = mongoPort

  @Value("${menas.mongo.connection.database}")
  val database: String = ""

  @PostConstruct
  def runDummyMongo(): Unit = {
    val starter = MongodStarter.getDefaultInstance

    synchronized {
      mongoPort = Network.getFreeServerPort()
      val mongodConfig = new MongodConfigBuilder()
        .version(Version.Main.V4_0)
        .net(new Net("localhost", mongoPort, Network.localhostIsIPv6()))
        .build()

      mongodExecutable = starter.prepare(mongodConfig)
    }

    mongodExecutable.start()
    logger.debug(s"*** mongod started at port $mongoPort")
  }

  @PreDestroy
  def shutdownDummyMongo(): Unit = {
    mongodExecutable.stop()
  }

  @Primary // will override non-primary MongoDatabase-typed bean when in scope - here: the 'defaultMongoDb' bean
  @Bean
  def embeddedMongoDb: MongoDatabase = {
    MongoClient(getMongoUri).getDatabase(database).withCodecRegistry(codecRegistry)
  }

}
