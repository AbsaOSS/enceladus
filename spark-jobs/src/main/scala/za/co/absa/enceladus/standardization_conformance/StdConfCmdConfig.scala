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

package za.co.absa.enceladus.standardization_conformance

import za.co.absa.enceladus.common.{JobCmdConfig, PathConfig}
import za.co.absa.enceladus.conformance.{ConfCmdConfigT, ConfConfig}
import za.co.absa.enceladus.standardization.{StdCmdConfigT, StdConfig}


abstract class StdConfCmdConfigT extends StdCmdConfigT with ConfCmdConfigT
case class StdConfCmdConfig(override val stdConfig: StdConfig = StdConfig(),
                            override val confConfig: ConfConfig = ConfConfig(),
                            override val jobConfig: JobCmdConfig = JobCmdConfig()) extends StdConfCmdConfigT

object StdConfCmdConfigT {

  val stepName = "StandardizationConformance"

  def getCmdLineArguments(args: Array[String]): StdConfCmdConfigT = {
    val jobConfig = JobCmdConfig.getCmdLineArguments(args, stepName)
    val stdConfig = StdConfig.getCmdLineArguments(args)
    val confConfig = ConfConfig.getCmdLineArguments(args)

    StdConfCmdConfig(stdConfig, confConfig, jobConfig)
  }
}
