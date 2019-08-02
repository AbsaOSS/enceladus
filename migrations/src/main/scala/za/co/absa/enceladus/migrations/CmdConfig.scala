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

package za.co.absa.enceladus.migrations

import scopt.OptionParser

/**
  * This is a class for configuration provided by the command line parameters to the command line Migration Tool
  *
  * Note: scopt requires all fields to have default values.
  * Even if a field is mandatory it needs a default value.
  */
case class CmdConfig (mongoDbURL: String = "",
                      database: String = "",
                      targetVersion: Int = -1)

object CmdConfig {

  def getCmdLineArguments(args: Array[String]): CmdConfig = {
    val parser = new CmdParser("java -cp MigrationsBundle.jar " +
      "za.co.absa.enceladus.migrations.MongoMigratorApp " +
      "--mongodb-url <MongoDb URL> --database <Database Name> --new-db-version <New DB Version>")

    val optionCmd = parser.parse(args, CmdConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  private class CmdParser(programName: String) extends OptionParser[CmdConfig](programName) {
    head("\nStandardization", "")
    var rawFormat: Option[String] = None

    opt[String]('U', "mongodb-url").required().action((value, config) =>
      config.copy(mongoDbURL = value)).text("A MongoDB URL")

    opt[String]('D', "database").required().action((value, config) =>
      config.copy(database = value)).text("A Database name")

    opt[Int]('N', "new-db-version").required().action((value, config) =>
      config.copy(targetVersion = value)).text("A new DB version number")

    help("help").text("prints this usage text")
  }

}

