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

package za.co.absa.enceladus.utils.validation

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ValidationLevelSuite extends AnyFunSuite {

  // these two simple steps shoudl prevent an unintended default validation level change without thinking through the consequences

  test("Verify enumeration names") {
    // this ensures that the defined constant stays correct - as the `toString` function cannot be used in annotations, even indirectly
    ValidationLevel.Constants.DefaultValidationLevelName should be (ValidationLevel.NoValidation.toString)
  }

  test("Verify default validation level") {
    ValidationLevel.Constants.DefaultValidationLevel.toString should be (ValidationLevel.Constants.DefaultValidationLevelName)
  }
}
