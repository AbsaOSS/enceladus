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

import java.sql.Timestamp
import za.co.absa.enceladus.utils.types.parsers.DateTimeParser
import scala.util.control.NonFatal

/**
  * This objects contains validators for date and time values and patterns
  */
object DateTimeValidator {

  // This is an example date that can be used for checking pattern conversion
  private val exampleDate: Timestamp = new Timestamp(System.currentTimeMillis)

  /**
    * Checks if date/time pattern is valid and can be used, i.e., using it does not raise an exception.
    * Also checks if a default value matches the pattern.
    *
    * @param pattern A date/time pattern
    * @param default An optional default value
    * @return None if no validation errors or Some(String) an error message
    */
  def isDateTimePatternValid(pattern: String, default: Option[String] = None): Option[ValidationIssue] ={
    try {
      // Checking pattern syntax
      val parser = DateTimeParser(pattern)
      // Checking pattern's ability to be used in formatting date/time values
      parser.format(exampleDate)
      // Checking default value correctness
      default.map( parser.parseDate )
      // Success
      None
    }
    catch {
      case NonFatal(e) => Some(ValidationError(e.getMessage))
    }
  }
}
