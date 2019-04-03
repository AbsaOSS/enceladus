/*
 * Copyright 2018-2019 ABSA Group Limited
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
  implicit class StringImprovements(string: String) {
    def findFirstUnquoted(charsToFind: Set[Char], quoteChars: Set[Char], escape: Char = '\\' ): Option[Integer] = {
      @tailrec
      def scan(sub: String, index: Integer, outQuote: Option[Char], escaped: Boolean = false):Option[Integer] = {
        //escaped flag defaults to false, as every non-escape character clears it
        val head: Option[Char] = sub.find(_ => true) //get the first character of the string if it exists
        (head, escaped) match {
          case (None, _) => None // end of string
          case (`outQuote`, false) if !outQuote.contains(escape)  => scan(sub.tail, index + 1, None) // end of quote,
                                                                                                     // unless escaped
                                                                                                     // or equals escape
          case (`outQuote`, true) if outQuote.contains(escape) => scan(sub.tail, index + 1, None) // end of quote set
                                                                                                  // by from escape
          case (Some(`escape`), false) => scan(sub.tail, index + 1, outQuote, escaped = true) // this is the only case
                                                                                              // when escape
          case (_, _) if outQuote.nonEmpty => scan(sub.tail, index + 1, outQuote) // within quotes
          //in follow-up cases, we have character and are not inside quotes
          case (Some(`escape`), true) if quoteChars.contains(escape) => scan(sub.tail, index + 1, Some(escape))
                                                                                   //entering quotes from escaped escape
          case (Some(`escape`), true) if charsToFind.contains(escape) => Some(index) //found hit in the form of escape
                                                                                     // character
          case (Some(c), false) if quoteChars.contains(c) => scan(sub.tail, index + 1, Some(c)) // entering quotes
          case (Some(c), false) if charsToFind.contains(c) => Some(index) //found hit
          case (Some(c), true) if quoteChars.contains(c) && charsToFind.contains(c) => Some(index) //found a hit in the
                                                                                                   // form of escaped
                                                                                                   // quote char
          case _ => scan(sub.tail, index + 1, None)
        }
      }

      if (charsToFind.contains(escape) && quoteChars.contains(escape)) {
        throw new InvalidParameterException(
          s"Escape character '$escape 'is both between charsToFind and quoteChars. That's not allowed."
        )
      }

      scan(string, 0, outQuote = None)
    }

    def hasUnquoted(charsToFind: Set[Char], quoteChars: Set[Char], escape: Char = '\\' ): Boolean = {
      findFirstUnquoted(charsToFind, quoteChars, escape).nonEmpty
    }
  }
}
