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

package za.co.absa.enceladus.standardization

import za.co.absa.enceladus.common.JobCmdConfig

/**
 * This is a class for configuration provided by the command line parameters
 *
 * Note: scopt requires all fields to have default values.
 * Even if a field is mandatory it needs a default value.
 */
case class StdCmdConfig(stdConfig: StdConfig = StdConfig(),
                        jobConfig: JobCmdConfig = JobCmdConfig())

object StdCmdConfig {

  val stepName = "Standardization"

  def getCmdLineArguments(args: Array[String]): StdCmdConfig = {
    val jobConfig = JobCmdConfig.getCmdLineArguments(args, stepName)
    val stdConfig = StdConfig.getCmdLineArguments(args)

    StdCmdConfig(stdConfig, jobConfig)
  }
}
