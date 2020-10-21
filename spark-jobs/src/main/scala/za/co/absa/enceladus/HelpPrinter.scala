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

package za.co.absa.enceladus

import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.standardization_conformance.config.StandardizationConformanceConfig

object HelpPrinter {
  def main(args: Array[String]): Unit = {
    val standardization_conformance = "za.co.absa.enceladus.standardization_conformance.StandardizationAndConformanceJob"
    val standardization = "za.co.absa.enceladus.standardization.StandardizationJob"
    val conformance = "za.co.absa.enceladus.conformance.DynamicConformanceJob"
    args.headOption.getOrElse(standardization_conformance) match {
      case `standardization` => StandardizationConfig.getFromArguments(Array("--help"))
      case `conformance` => ConformanceConfig.getFromArguments(Array("--help"))
      case `standardization_conformance` => StandardizationConformanceConfig.getFromArguments(Array("--help"))
    }
  }
}
