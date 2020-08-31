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

package za.co.absa.enceladus.common

import org.scalatest.FunSuite
import za.co.absa.atum.core.ControlType
import za.co.absa.atum.model.{Checkpoint, Measurement}

import scala.util.Success

class ControlInfoValidationSuite extends FunSuite {
  import za.co.absa.atum.core.Constants._

  private val checkpoints1 = List(
    Checkpoint("raw", None, None, "", "", "", 0, List(
      Measurement("", ControlType.AbsAggregatedTotal.value, "", 0),
      Measurement("",  ControlType.Count.value, "", 11)
    )
    ),
    Checkpoint("source", None, None, "", "", "", 1, List(
      Measurement("", ControlType.Count.value, "", 3)
    )
    )
  )

  private val checkpoints2 = List(
    Checkpoint("source", None, None, "", "", "", 1, List(
      Measurement("", ControlType.DistinctCount.value, "", 1)
    )
    )
  )

  private val checkpoints3 = List(
    Checkpoint("raw", None, None, "", "", "", 0, List(
      Measurement("", ControlType.Count.value, "", -3)
    )
    ),
    Checkpoint("source", None, None, "", "", "", 1, List(
      Measurement("", ControlType.Count.value, "", "")
    )
    )
  )

  test("Correct values") {
    val rawResult = ControlInfoValidation.getCountFromGivenCheckpoint("raw", checkpoints1)
    val sourceResult = ControlInfoValidation.getCountFromGivenCheckpoint("source", checkpoints1)

    assert(rawResult == Success(11))
    assert(sourceResult == Success(3))
  }

  test("Errors in parsing") {
    val rawResult = ControlInfoValidation.getCountFromGivenCheckpoint("raw", checkpoints2)
    val sourceResult = ControlInfoValidation.getCountFromGivenCheckpoint("source", checkpoints2)

    val rawError = "Missing raw checkpoint"
    val sourceError = s"source checkpoint does not have a ${ControlType.Count.value} control"

    assert(rawResult.failed.get.getMessage == rawError)
    assert(sourceResult.failed.get.getMessage == sourceError)
  }

  test("Wrong controlValue values") {
    val rawResult = ControlInfoValidation.getCountFromGivenCheckpoint("raw", checkpoints3)
    val sourceResult = ControlInfoValidation.getCountFromGivenCheckpoint("source", checkpoints3)

    val rawError = s"Wrong raw ${ControlType.Count.value} value: Negative value"
    val sourceError = s"""Wrong source ${ControlType.Count.value} value: For input string: \"\""""

    assert(rawResult.failed.get.getMessage == rawError)
    assert(sourceResult.failed.get.getMessage == sourceError)
  }

}
