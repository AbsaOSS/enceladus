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
    s"'$input' should produce '$expected'" in {
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
