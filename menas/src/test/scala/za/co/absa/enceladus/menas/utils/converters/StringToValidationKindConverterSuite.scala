package za.co.absa.enceladus.menas.utils.converters

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import za.co.absa.enceladus.menas.utils.enumerations.ValidationKind._

class StringToValidationKindConverterSuite extends AnyWordSpec {

  private val converter = new StringToValidationKindConverter

  Seq(
    ("NoValidation", NoValidation),
    ("ForRun", ForRun),
    ("Strictest", Strictest)
  ).foreach { case (input, expected) =>
    s"'$input' should produce `$expected``" in {
        val result = converter.convert(input)
        result shouldBe expected
    }
  }

  "Exception should be fired for wrong input" in {
    intercept[NoSuchElementException] {
      converter.convert("This is wrong")
    }
  }
}
