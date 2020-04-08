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

package za.co.absa.enceladus.menas

import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class MongoConfig {
  import za.co.absa.enceladus.menas.utils.implicits._

  @Value("${menas.mongo.connection.string}")
  val connectionString: String = ""
  @Value("${menas.mongo.connection.database}")
  val database: String = ""

  def mongoClient: MongoClient = MongoClient(connectionString)

  @Bean
  def mongoDb: MongoDatabase = mongoClient.getDatabase(database).withCodecRegistry(codecRegistry)

}
