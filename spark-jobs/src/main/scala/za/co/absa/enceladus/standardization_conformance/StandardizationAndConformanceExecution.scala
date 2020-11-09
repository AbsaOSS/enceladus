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

import org.apache.hadoop.conf.Configuration
import za.co.absa.enceladus.common.CommonJobExecution
import za.co.absa.enceladus.common.config.{JobConfigParser, PathConfig}
import za.co.absa.enceladus.conformance.ConformanceExecution
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.StandardizationExecution
import za.co.absa.enceladus.standardization_conformance.config.StandardizationConformanceConfig
import za.co.absa.enceladus.utils.config.PathWithFs

trait StandardizationAndConformanceExecution extends StandardizationExecution
  with ConformanceExecution
  with CommonJobExecution {

  override def getPathConfig[T](cmd: JobConfigParser[T], dataset: Dataset, reportVersion: Int)
                               (implicit hadoopConf: Configuration): PathConfig = {
    val defaultConfig = super[CommonJobExecution].getPathConfig(cmd, dataset, reportVersion)
    val jobCmd = cmd.asInstanceOf[StandardizationConformanceConfig]
    val rawPathOverride = jobCmd.rawPathOverride
    val publishPathOverride = jobCmd.publishPathOverride
    defaultConfig.copy(
      raw = PathWithFs.fromPath(rawPathOverride.getOrElse(defaultConfig.raw.path)),
      publish = PathWithFs.fromPath(publishPathOverride.getOrElse(defaultConfig.publish.path))
    )
  }

  override def validateOutputPath(pathConfig: PathConfig): Unit = {
    // Std output is validated in the std FS
    validateIfPathAlreadyExists(pathConfig.standardization)

    // publish output is validated in the publish FS
    validateIfPathAlreadyExists(pathConfig.publish)
  }
}
