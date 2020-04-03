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

package za.co.absa.enceladus.menas.integration.mongo


import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.process.runtime.Network
import org.scalatest.{BeforeAndAfter, Suite}

/**
 * Provides an embedded local mongo. Spawn it before tests and shutdown after
 */
trait EmbeddedMongoSuite /*extends BeforeAndAfter*/ { /*this: Suite =>*/
  private var mongodExecutable: MongodExecutable = _
  //private var mongoPort: Int = _
  val mongoPort = 27077

  def getMongoUri: String = s"mongodb://localhost:$mongoPort/?ssl=false"
  def getMongoPort: Int = mongoPort

  def runDummyMongo(): MongodProcess = {
    val starter = MongodStarter.getDefaultInstance

    synchronized {
      // mongoPort = Network.getFreeServerPort()
      val mongodConfig = new MongodConfigBuilder()
        .version(Version.Main.V4_0)
        .net(new Net("localhost", mongoPort, Network.localhostIsIPv6()))
        .build()

      mongodExecutable = starter.prepare(mongodConfig)
    }

    mongodExecutable.start()
  }

  def shutdownDummyMongo(): Unit = {
    mongodExecutable.stop()
  }

  /*
  before {
    runDummyMongo()
  }

  after {
    shutdownDummyMongo()
  }

   */

}
