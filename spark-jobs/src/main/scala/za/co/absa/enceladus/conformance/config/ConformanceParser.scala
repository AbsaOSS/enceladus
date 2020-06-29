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

package za.co.absa.enceladus.conformance.config

import scopt.OParser
import za.co.absa.enceladus.common.config.JobParser

trait ConformanceParser[R] extends JobParser[R] {
  def publishPathOverride: Option[String]
  def experimentalMappingRule: Option[Boolean]
  def isCatalystWorkaroundEnabled: Option[Boolean]
  def autocleanStandardizedFolder: Option[Boolean]

  def withPublishPathOverride(vlue: Option[String]): R
  def withExperimentalMappingRule(value: Option[Boolean]): R
  def withIsCatalystWorkaroundEnabled(value: Option[Boolean]): R
  def withAutocleanStandardizedFolder(value: Option[Boolean]): R
}

object ConformanceParser {

  def conformanceParser[R <: ConformanceParser[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]
    import builder._
    OParser.sequence(
    head("Dynamic Conformance", ""),

    opt[String]("debug-set-publish-path").optional().hidden().action((value, config) =>
      config.withPublishPathOverride(Some(value))).text("override the path of the published data (used internally for testing)"),

    opt[Boolean]("experimental-mapping-rule").optional().action((value, config) =>
      config.withExperimentalMappingRule(Option(value))).text("Use experimental optimized mapping conformance rule"),

    opt[Boolean]("catalyst-workaround").optional().action((value, config) =>
      config.withIsCatalystWorkaroundEnabled(Some(value))).text("Turn on or off Catalyst workaround feature. " +
      "This overrides 'conformance.catalyst.workaround' configuration value provided in 'application.conf'."),

    opt[Boolean]("autoclean-std-folder").optional().action((value, config) =>
      config.withAutocleanStandardizedFolder(Option(value))).text("Deletes standardized data from HDFS once " +
      "it is successfully conformed. This overrides 'conformance.autoclean.standardized.hdfs.folder' configuration " +
      " value provided in 'application.conf'.")
    )
  }
}

