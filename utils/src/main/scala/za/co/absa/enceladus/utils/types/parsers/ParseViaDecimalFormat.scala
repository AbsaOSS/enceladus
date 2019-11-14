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

package za.co.absa.enceladus.utils.types.parsers

import java.text.{DecimalFormat, ParsePosition}

import za.co.absa.enceladus.utils.types.parsers.NumericParser.NumericParserException

import scala.util.{Failure, Success, Try}

/**
  * Trait to implement the common logic of parsing and formatting using DecimalFormat object
 *
  * @tparam N a numeric types
  */
trait ParseViaDecimalFormat[N] {
  protected val decimalFormat: Option[DecimalFormat]
  protected val numberConversion: Number => N

  protected def parseUsingPattern(stringToParse: String):Try[N] = {
    import za.co.absa.enceladus.utils.implicits.OptionImplicits.OptionEnhancements

    def checkPosAtEnd(pos: ParsePosition): Try[Unit] = {
      if (pos.getIndex < stringToParse.length) {
        Failure(new NumericParserException(s"Parsing of '$stringToParse' failed."))
      } else {
        Success(Unit)
      }
    }

    for {
      formatter <- decimalFormat.toTry(new NumericParserException("No pattern provided"))
      pos       = new ParsePosition(0)
      parsed    <- Try(formatter.parse(stringToParse, pos))
      _         <- checkPosAtEnd(pos)
      result    <- Try(numberConversion(parsed))
    } yield result
  }

  protected def formatUsingPattern(value: N): String = {
    decimalFormat.map(_.format(value).trim).getOrElse(value.toString)
  }
}
