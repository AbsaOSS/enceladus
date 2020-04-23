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

import za.co.absa.atum.core.Atum
import za.co.absa.atum.model.Checkpoint
import za.co.absa.enceladus.utils.validation.ValidationException
import za.co.absa.enceladus.utils.implicits.OptionImplicits._

import scala.util.{Failure, Success, Try}

object ControlInfoValidation {
  private val rawFieldName = "raw"
  private val sourceFieldName = "source"

  case class FieldCounts(rawCount: Long, sourceCount: Long)

  /**
   * Adds metadata about the number of records in raw and source data by checking Atum's checkpoints first.
   */
  def addRawAndSourceRecordCountsToMetadata(): Unit = {
    val fields = getFieldCounts(Atum.getControlMeasure.checkpoints)
    Atum.setAdditionalInfo(s"${rawFieldName}_record_count" -> fields.rawCount.toString)
    Atum.setAdditionalInfo(s"${sourceFieldName}_record_count" -> fields.sourceCount.toString)
  }

  def getFieldCounts(checkpoints: Seq[Checkpoint]): FieldCounts = {
    val checkpointRawRecordCount = getCountFromGivenCheckpoint(rawFieldName, checkpoints)
    val checkpointSourceRecordCount = getCountFromGivenCheckpoint(sourceFieldName, checkpoints)
    val errorMessage = "Checkpoint validation failed:"

    (checkpointRawRecordCount, checkpointSourceRecordCount) match {
      case (Success(x), Success(y)) => FieldCounts(x, y)
      case (Success(_), Failure(er)) => throw new ValidationException(errorMessage, Seq(er.getMessage))
      case (Failure(er), Success(_)) => throw new ValidationException(errorMessage, Seq(er.getMessage))
      case (Failure(er1), Failure(er2)) =>
        throw new ValidationException(errorMessage, Seq(er1.getMessage, er2.getMessage))
    }
  }


  def getCountFromGivenCheckpoint(checkpointName: String, checkpoints: Seq[Checkpoint]): Try[Long] = {
    import za.co.absa.atum.core.Constants._

    for {
      checkpoint <- checkpoints
        .find(c => c.name.equalsIgnoreCase(checkpointName) || c.workflowName.equalsIgnoreCase(checkpointName))
        .toTry(new Exception(s"Missing $checkpointName checkpoint"))
      measurement <- checkpoint.controls.find(m => m.controlType.equalsIgnoreCase(controlTypeRecordCount))
        .toTry(new Exception(s"$checkpointName checkpoint does not have a $controlTypeRecordCount control"))
      res <- Try {
        val rowCount = measurement.controlValue.toString.toLong
        if (rowCount >= 0) rowCount else throw new Exception(s"Negative value")
      }.recoverWith {
        case t: Throwable =>
          Failure(new Exception(s"Wrong $checkpointName $controlTypeRecordCount value: ${t.getMessage}"))
      }
    } yield res

  }

}
