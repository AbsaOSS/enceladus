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

package za.co.absa.enceladus.conformance.interpreter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import za.co.absa.enceladus.conformance.config.{ConformanceConfig, ConformanceParser}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.{Dataset => ConfDataset}
import za.co.absa.enceladus.standardization_conformance.config.StdConformanceConfig

/** Holds everything that is needed in between dynamic conformance interpreter stages */

case class InterpreterContextArgs(datasetName: String,
                                  reportDate: String = "",
                                  persistStorageLevel: Option[StorageLevel] = None)

object InterpreterContextArgs {
  def fromConformanceConfig[T](conformanceConfig: ConformanceParser[T]): InterpreterContextArgs = {

    conformanceConfig match {
      case ConformanceConfigInstanceInterpreter(interpreterContextArgs) => interpreterContextArgs
      case StdConformanceConfigInstanceInterpreter(interpreterContextArgs) => interpreterContextArgs
      case _ => throw new Exception("")
    }
  }
}

object ConformanceConfigInstanceInterpreter {
  def unapply(conformanceInstance: ConformanceConfig): Option[InterpreterContextArgs] =
    Some(InterpreterContextArgs(conformanceInstance.datasetName: String, conformanceInstance.reportDate: String,
      conformanceInstance.persistStorageLevel: Option[StorageLevel]))
}

object StdConformanceConfigInstanceInterpreter {
  def unapply(conformanceInstance: StdConformanceConfig): Option[InterpreterContextArgs] =
    Some(InterpreterContextArgs(conformanceInstance.datasetName: String, conformanceInstance.reportDate: String,
      conformanceInstance.persistStorageLevel: Option[StorageLevel]))
}

case class InterpreterContext(
                               schema: StructType,
                               conformance: ConfDataset,
                               featureSwitches: FeatureSwitches,
                               jobShortName: String,
                               spark: SparkSession,
                               dao: MenasDAO,
                               progArgs: InterpreterContextArgs
                             )
