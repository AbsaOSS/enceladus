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

import org.scalatest.FunSuite
import za.co.absa.atum.model.{Checkpoint, Measurement}
import za.co.absa.enceladus.standardization.ControlInfoValidation.FieldCounts
import za.co.absa.enceladus.utils.validation.ValidationException
import scala.util.Success

class ControlInfoValidationSuite extends FunSuite {

  import za.co.absa.atum.core.Constants._

  val checkpoints1 = List(
    Checkpoint("raw", "", "", "", 0, List(
      Measurement("", controlTypeAbsAggregatedTotal, "", 0),
      Measurement("", controlTypeRecordCount, "", 11)
    )
    ),
    Checkpoint("source", "", "", "", 1, List(
      Measurement("", controlTypeRecordCount, "", 3)
    )
    )
  )

  val checkpoints2 = List(
    Checkpoint("source", "", "", "", 1, List(
      Measurement("", controlTypeDistinctCount, "", 1)
    )
    )
  )

  val checkpoints3 = List(
    Checkpoint("raw", "", "", "", 0, List(
      Measurement("", controlTypeRecordCount, "", -3)
    )
    ),
    Checkpoint("source", "", "", "", 1, List(
      Measurement("", controlTypeRecordCount, "", "")
    )
    )
  )

  test("Correct values") {
    val rawResult = ControlInfoValidation.getFieldRecordFromCheckpoints("raw", checkpoints1)
    val sourceResult = ControlInfoValidation.getFieldRecordFromCheckpoints("source", checkpoints1)
    val counts = ControlInfoValidation.getFieldCounts(checkpoints1)

    assert(rawResult == Success(11))
    assert(sourceResult == Success(3))
    assert(counts == FieldCounts(11, 3))
  }

  test("Errors in parsing") {
    val rawResult = ControlInfoValidation.getFieldRecordFromCheckpoints("raw", checkpoints2)
    val sourceResult = ControlInfoValidation.getFieldRecordFromCheckpoints("source", checkpoints2)
    assertThrows[ValidationException](ControlInfoValidation.getFieldCounts(checkpoints2))

    rawResult.failed.get.getMessage
    assert(rawResult.failed.get.getMessage == "Missing raw checkpoint")
    assert(sourceResult.failed.get.getMessage == s"source checkpoint does not have a $controlTypeRecordCount control")
  }

  test("Wrong controlValue values") {
    val rawResult = ControlInfoValidation.getFieldRecordFromCheckpoints("raw", checkpoints3)
    val sourceResult = ControlInfoValidation.getFieldRecordFromCheckpoints("source", checkpoints3)
    assertThrows[ValidationException](ControlInfoValidation.getFieldCounts(checkpoints3))

    assert(rawResult.failed.get.getMessage == s"Wrong raw $controlTypeRecordCount value: Negative value")
    assert(sourceResult.failed.get.getMessage ==
      s"""Wrong source $controlTypeRecordCount value: For input string: \"\"""")
  }


}
