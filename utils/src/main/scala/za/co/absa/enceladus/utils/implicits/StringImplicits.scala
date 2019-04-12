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
    /**
      * Function to find the first occurrence of any of the characters from the charsToFind in the string. The
      * occurrence is not considered if the character is part of a sequence within a pair of quote characters specified
      * by quoteChars param.
      * Escape character in front of a quote character will cancel its "quoting" function.
      * Escape character in front of a searched-for character will not result in positive identification of a find
      * Double escape character is considered to be escape character itself, without its special meaning
      * The escape character can be part of the charsToFind set or quoteChars set and the function will work as
      * expected (e.g. double escape character being recognized as a searched-for character or quote character), but it
      * cannot be both - that will fire an exception.
      * @param charsToFind set of characters to look for
      * @param quoteChars set of characters that are considered as quotes, everything within two (same) quote characters
      *                   is not considered
      * @param escape the special character to escape the expected behavior within string
      * @return the index of the first find within the string, or None in case of no find
      */
    def findFirstUnquoted(charsToFind: Set[Char], quoteChars: Set[Char], escape: Char = '\\' ): Option[Integer] = {
      @tailrec
      def scan(sub: String, idx: Integer, charToExitQuotes: Option[Char], escaped: Boolean = false): Option[Integer] = {
        //escaped flag defaults to false, as every non-escape character clears it
        val head: Option[Char] = sub.find(_ => true) //get the first character of the string if it exists
        (head, escaped) match {
          // end of string
          case (None, _) => None
          // following cases are to address situations when the head character is within quotes (not yet closed)
            // exit quote unless it's escaped or is the escape character itself
          case (`charToExitQuotes`, false) if !charToExitQuotes.contains(escape) => scan(sub.tail, idx + 1, None)
            // escaped exit quote means exit only if it's the escape character itself
          case (`charToExitQuotes`, true) if charToExitQuotes.contains(escape) => scan(sub.tail, idx + 1, None)
            // escape charter found (not necessary withing quotes, but has to be handled it this order)
          case (Some(`escape`), false) => scan(sub.tail, idx + 1, charToExitQuotes, escaped = true)
            // any other character within quotes, no special case
          case _ if charToExitQuotes.nonEmpty => scan(sub.tail, idx + 1, charToExitQuotes)
          // following cases addresses situations when the head character is outside quotes
            //escaped escape character if it's also a quote character
          case (Some(`escape`), true) if quoteChars.contains(escape) => scan(sub.tail, idx + 1, Some(escape))
            //escaped escape character if it's also a character to find
          case (Some(`escape`), true) if charsToFind.contains(escape) => Some(idx)
            // entering quotes
          case (Some(c), false) if quoteChars.contains(c) => scan(sub.tail, idx + 1, Some(c)) // entering quotes
            // found one of the characters to search for
          case (Some(c), false) if charsToFind.contains(c) => Some(idx)
            // an escaped quote character that is also within the characters to find
          case (Some(c), true) if quoteChars.contains(c) && charsToFind.contains(c) => Some(idx)
            //all other cases, continue scan
          case   _ => scan(sub.tail, idx + 1, None)
        }
      }

      if (charsToFind.contains(escape) && quoteChars.contains(escape)) {
        throw new InvalidParameterException(
          s"Escape character '$escape 'is both between charsToFind and quoteChars. That's not allowed."
        )
      }

      scan(string, 0, charToExitQuotes = None)
    }

    /**
      * Similar to above, only te result is true if anything is found, false otherwise
      * @param charsToFind set of characters to look for
      * @param quoteChars set of characters that are considered as quotes, everything within two (same) quote characters
      *                   is not considered
      * @param escape the special character to escape the expected behavior within string
      * @return true if anything is found, false otherwise
      */
    def hasUnquoted(charsToFind: Set[Char], quoteChars: Set[Char], escape: Char = '\\' ): Boolean = {
      findFirstUnquoted(charsToFind, quoteChars, escape).nonEmpty
    }
  }
}
