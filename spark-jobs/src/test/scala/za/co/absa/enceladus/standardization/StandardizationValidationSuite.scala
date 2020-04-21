package za.co.absa.enceladus.standardization

import org.scalatest.FunSuite
import za.co.absa.atum.model.{Checkpoint, Measurement}

class StandardizationValidationSuite extends FunSuite {
  import za.co.absa.atum.core.Constants._

  val checkpoints1 = List(
    Checkpoint("raw", "", "", "", 0, List(
        Measurement("",controlTypeAbsAggregatedTotal,"",0),
        Measurement("",controlTypeRecordCount,"",11)
      )
    ),
    Checkpoint("source", "", "", "", 1, List(
      Measurement("",controlTypeRecordCount,"",3)
    )
    )
  )

  val checkpoints2 = List(
    Checkpoint("source", "", "", "", 1, List(
      Measurement("",controlTypeDistinctCount,"",1)
    )
    )
  )

  val checkpoints3 = List(
    Checkpoint("raw", "", "", "", 0, List(
      Measurement("",controlTypeRecordCount,"",-3)
    )
    ),
    Checkpoint("source", "", "", "", 1, List(
      Measurement("",controlTypeRecordCount,"","")
    )
    )
  )

  test("Correct values") {
    val rawResult = StandardizationJob.getRawRecordCountFromCheckpoints(checkpoints1)
    val sourceResult = StandardizationJob.getSourceRecordCountFromCheckpoints(checkpoints1)

    assert(rawResult == Right(11))
    assert(sourceResult == Right(3))
  }

  test("Errors in parsing") {
    val rawResult = StandardizationJob.getRawRecordCountFromCheckpoints(checkpoints2)
    val sourceResult = StandardizationJob.getSourceRecordCountFromCheckpoints(checkpoints2)

    assert(rawResult == Left("Missing raw checkpoint"))
    assert(sourceResult == Left(s"source checkpoint does not have a $controlTypeRecordCount control"))
  }

  test("Wrong controlValue values") {
    val rawResult = StandardizationJob.getRawRecordCountFromCheckpoints(checkpoints3)
    val sourceResult = StandardizationJob.getSourceRecordCountFromCheckpoints(checkpoints3)

    assert(rawResult == Left(s"Wrong raw $controlTypeRecordCount value: Negative value"))
    assert(sourceResult == Left(s"""Wrong source $controlTypeRecordCount value: For input string: \"\""""))
  }

}
