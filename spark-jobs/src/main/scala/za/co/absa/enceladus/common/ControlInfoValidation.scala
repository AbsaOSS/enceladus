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

import za.co.absa.atum.core.{Atum, ControlType}
import za.co.absa.atum.model.Checkpoint
import za.co.absa.enceladus.utils.implicits.OptionImplicits._
import za.co.absa.enceladus.utils.validation.ValidationException

import scala.util.{Failure, Success, Try}

object ControlInfoValidation {
  private val rawFieldName = "raw"
  private val sourceFieldName = "source"

  /**
   * Adds metadata about the number of records in raw and source data by checking Atum's checkpoints first.
   * @return Validation results
   */

  def addRawAndSourceRecordCountsToMetadata(): Try[Unit] = {
    val checkpoints = Atum.getControlMeasure.checkpoints
    val checkpointRawRecordCount = getCountFromGivenCheckpoint(rawFieldName, checkpoints)
    val checkpointSourceRecordCount = getCountFromGivenCheckpoint(sourceFieldName, checkpoints)

    checkpointRawRecordCount
      .foreach(e => Atum.setAdditionalInfo(s"${rawFieldName}_record_count" -> e.toString))

    checkpointSourceRecordCount
      .foreach(e => Atum.setAdditionalInfo(s"${sourceFieldName}_record_count" -> e.toString))

    validateFields(checkpointRawRecordCount, checkpointSourceRecordCount)
  }

  private def validateFields(checkpointRawRecordCount: Try[Long],
                             checkpointSourceRecordCount: Try[Long]): Try[Unit] = {
    val errorMessage = "Checkpoint validation failed: "

    (checkpointRawRecordCount, checkpointSourceRecordCount) match {
      case (Success(_), Failure(er)) =>
        Failure(new ValidationException(s"$errorMessage ${er.getMessage}", Seq(er.getMessage)))
      case (Failure(er), Success(_)) =>
        Failure(new ValidationException(s"$errorMessage ${er.getMessage}", Seq(er.getMessage)))
      case (Failure(er1), Failure(er2)) =>
        Failure(new ValidationException(s"$errorMessage ${er1.getMessage}, ${er2.getMessage}",
          Seq(er1.getMessage, er2.getMessage)))
      case (_, _) => Success(Unit)
    }
  }


  def getCountFromGivenCheckpoint(checkpointName: String, checkpoints: Seq[Checkpoint]): Try[Long] = {
    import za.co.absa.atum.core.Constants._

    for {
      checkpoint <- checkpoints
        .find(c => c.name.equalsIgnoreCase(checkpointName) || c.workflowName.equalsIgnoreCase(checkpointName))
        .toTry(new Exception(s"Missing $checkpointName checkpoint"))
      measurement <- checkpoint.controls.find(m => ControlType.isControlMeasureTypeEqual(m.controlType, ControlType.Count.value))
        .toTry(new Exception(s"$checkpointName checkpoint does not have a ${ControlType.Count.value} control"))
      res <- Try {
        val rowCount = measurement.controlValue.toString.toLong
        if (rowCount >= 0) rowCount else throw new Exception(s"Negative value")
      }.recoverWith {
        case t: Throwable =>
          Failure(new Exception(s"Wrong $checkpointName ${ControlType.Count.value} value: ${t.getMessage}"))
      }
    } yield res

  }

}
