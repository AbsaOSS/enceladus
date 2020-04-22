package za.co.absa.enceladus.standardization

import za.co.absa.atum.core.Atum
import za.co.absa.atum.model.{Checkpoint, Measurement}
import za.co.absa.enceladus.utils.validation.ValidationException

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
    val checkpointRawRecordCount = getFieldRecordFromCheckpoints(rawFieldName, checkpoints)
    val checkpointSourceRecordCount = getFieldRecordFromCheckpoints(sourceFieldName, checkpoints)
    val errorMessage = "Checkpoint validation failed:"

    (checkpointRawRecordCount, checkpointSourceRecordCount) match {
      case (Success(x), Success(y)) => FieldCounts(x, y)
      case (Success(_), Failure(er)) => throw new ValidationException(errorMessage, Seq(er.getMessage))
      case (Failure(er), Success(_)) => throw new ValidationException(errorMessage, Seq(er.getMessage))
      case (Failure(er1), Failure(er2)) => throw new ValidationException(errorMessage, Seq(er1.getMessage, er2.getMessage))
    }
  }


  def getFieldRecordFromCheckpoints(checkpointName: String, checkpoints: Seq[Checkpoint]): Try[Long] = {
    import za.co.absa.atum.core.Constants._

    for {
      checkpoint <- Try (
        checkpoints
          .find(c => c.name.equalsIgnoreCase(checkpointName) || c.workflowName.equalsIgnoreCase(checkpointName))
          .getOrElse(throw new Exception(s"Missing $checkpointName checkpoint"))
        )
      measurement <- Try (
        checkpoint.controls.find(m => m.controlType.equalsIgnoreCase(controlTypeRecordCount))
          .getOrElse(throw new Exception(s"$checkpointName checkpoint does not have a $controlTypeRecordCount control"))
      )
      res <- Try {
        val rowCount = measurement.controlValue.toString.toLong
        if (rowCount >= 0) rowCount else throw new Exception(s"Negative value")
      }.recoverWith({
        case t: Throwable => Failure(new Exception(s"Wrong $checkpointName $controlTypeRecordCount value: ${t.getMessage}"))
      })
    } yield res

  }

}
