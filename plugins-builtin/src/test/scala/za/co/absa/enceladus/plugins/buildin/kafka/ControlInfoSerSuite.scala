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

package za.co.absa.enceladus.plugins.buildin.kafka

import org.apache.commons.io.IOUtils
import org.scalatest.FunSuite
import za.co.absa.enceladus.plugins.buildin.factories.DceControlInfoFactory
import za.co.absa.enceladus.plugins.builtin.kafka.ControlInfoAvroSerializer

class ControlInfoSerSuite extends FunSuite {
  test ("Control info key serialize to Avro") {
    val avroControlInfoKey = ControlInfoAvroSerializer.convertControlInfoKey("dummyDataset")

    assert(avroControlInfoKey.toString == """{"datasetName": "dummyDataset"}""")
  }

  test ("Control info metrics serialize to Avro") {
    val dceControlInfo = DceControlInfoFactory.getDummyDceControlInfo()
    val avroControlInfo = ControlInfoAvroSerializer.convertControlInfoRecord(dceControlInfo)
    val expectedAvroRecord = IOUtils.toString(getClass.getResourceAsStream("/test_data/dummyAcontrolInfoAvro.json"), "UTF-8")

    assert(avroControlInfo.isDefined)
    assert(avroControlInfo.get.toString == expectedAvroRecord)
  }
}
