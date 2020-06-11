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

import za.co.absa.enceladus.common.PathConfig
import za.co.absa.enceladus.conformance.{ConfCmdConfig, ConformanceExecution}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.{StandardizationExecution, StdCmdConfig}

trait StandardizationConformanceExecution extends StandardizationExecution with ConformanceExecution {

  def getFullPathCfg(cmd: StdConfCmdConfigT, dataset: Dataset, reportVersion: Int): PathConfig = {
    val forRawPath = StdCmdConfig(cmd.stdConfig, cmd.jobConfig)
    val forPublishPath = ConfCmdConfig(cmd.confConfig, cmd.jobConfig)
    val standardization = getStandardizationPath(cmd.jobConfig, reportVersion)

    PathConfig(
      inputPath = buildRawPath(forRawPath, dataset, reportVersion),
      outputPath = buildPublishPath(forPublishPath, dataset, reportVersion),
      standardizationPath = Some(standardization)
    )
  }

}
