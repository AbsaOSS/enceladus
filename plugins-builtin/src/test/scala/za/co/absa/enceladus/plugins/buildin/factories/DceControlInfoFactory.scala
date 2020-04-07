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

package za.co.absa.enceladus.plugins.buildin.factories

import za.co.absa.atum.model.{Checkpoint, ControlMeasure, ControlMeasureMetadata, Measurement}
import za.co.absa.enceladus.plugins.builtin.controlinfo.DceControlInfo

object DceControlInfoFactory {

  def getDummyControlMeasure(sourceApplication: String = "TestApp",
                             country: String = "Global",
                             historyType: String = "Snapshot",
                             dataFilename: String = "file1.csv",
                             sourceType: String = "dump",
                             version: Int = 1,
                             informationDate: String = "2019-12-21",
                             additionalInfo: Map[String, String] = Map[String, String]("key1" -> "value1", "key2" -> "value2")
                            ): ControlMeasure = {
    ControlMeasure(ControlMeasureMetadata(sourceApplication,
      country,
      historyType,
      dataFilename,
      sourceType,
      version,
      informationDate,
      additionalInfo), None, List(
      Checkpoint("Source", "2019-12-21 10:21:33", "2019-12-21 12:10:57", "Source", 1,
        List(Measurement("recordCount", "controlType.count", "*", "223929"))),
      Checkpoint("Raw", "2019-12-21 12:11:22", "2019-12-21 13:20:21", "Raw", 2,
        List(Measurement("recordCount", "controlType.count", "*", "223929")))
    ))
  }

  def getDummyDceControlInfo(controlMeasure: ControlMeasure = getDummyControlMeasure(),
                             datasetName: String = "dummyDataset",
                             datasetVersion: Int = 1,
                             reportDate: String = "2019-12-21",
                             reportVersion: Int = 1,
                             runStatus: String = "running")
  : DceControlInfo = {
    DceControlInfo(datasetName, datasetVersion, reportDate, reportVersion, runStatus, controlMeasure)
  }

}
