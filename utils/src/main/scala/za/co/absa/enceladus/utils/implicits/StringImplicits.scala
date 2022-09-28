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

package za.co.absa.enceladus.utils.implicits

import java.security.InvalidParameterException
import scala.annotation.tailrec

object StringImplicits {
  implicit class StringEnhancements(string: String) {

    /**
      * Splits the string around the provided delimiter, unless it's inside quotes
      * from https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
      *
      * @param delimiter the delimiting character
      * @param limit     the result threshold
      * @return the sequence of strings computed by splitting this string around the provided delimiter
      */
    def splitWithQuotes(delimiter: Char = ',', limit: Int = 0): Seq[String] = {
      if (string == "" && limit == 0) {
        // the regex doesn't work as desired on empty string
        Seq.empty
      } else {
        // make the separation
        val separationRegex = "\\" + delimiter + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
        string.split(separationRegex, limit)
      }
    }

    def trimStartEndChar(start: Char, end: Char): String = {
      val lastIndex = string.length - 1
      lastIndex match {
        case -1 => ""
        case 0 => string
        case x if string(0) == start && string(x) == end => string.substring(1, x)
        case _ => string
      }
    }

    def trimStartEndChar(startAndEnd: Char): String = {
      trimStartEndChar(startAndEnd, startAndEnd)
    }
  }
}
