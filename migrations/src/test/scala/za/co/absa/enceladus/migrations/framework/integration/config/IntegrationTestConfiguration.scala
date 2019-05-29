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

package za.co.absa.enceladus.migrations.framework.integration.config

import java.util.Properties

/**
  * This object gets integration suite environment-specific options.
  *
  * - MongoDB connection string and database name can be specified in 'application.properties'
  *   and/or as JVM properties (-D...)
  * - If a configuration key is present in both places JVM system properties take precedence.
  * - Migrations integration tests can be ran via 'mvn test -Pintegration'
  * - Integration tests expect the the database does not exist at startup.
  * - If integration tests are successful the database will be dropped automatically.
  */
object IntegrationTestConfiguration {
  private val properties = new Properties()
  private val connectionStringKey = "za.co.absa.enceladus.menas.mongo.connection.string"
  private val databaseKey = "za.co.absa.enceladus.migrations.mongo.connection.database"

  loadConfig()

  /**
    * Returns a connection string to a MongoDB instance to be used in integration tests.
    */
  def getMongoDbConnectionString: String = System.getProperty(connectionStringKey,
    properties.getProperty(connectionStringKey))

  /**
    * Returns a database name in integration tests.
    */
  def getMongoDbDatabase: String = System.getProperty(databaseKey,
    properties.getProperty(databaseKey))

  private def loadConfig(): Unit = {
    try {
      val is = getClass.getResourceAsStream("/application.properties")
      try
        properties.load(is)
      finally if (is != null) is.close()
    }
  }

}
