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

import java.util.Locale
import za.co.absa.enceladus.utils.numeric.{DecimalSymbols, NumericPattern}
import za.co.absa.enceladus.utils.types.parsers.NumericParser.NumericParserException
import scala.util.{Failure, Success, Try}

abstract class NumericParser[N: Ordering](val pattern: NumericPattern,
                                          val min: Option[N],
                                          val max: Option[N]) {

  protected val stringConversion: String => N

  protected def parseUsingPattern(stringToParse: String):Try[N]
  protected def formatUsingPattern(value: N): String

  def parse(string: String): Try[N] = {
    for {
      parsed <- pattern.specifiedPattern.map(_ => parseUsingPattern(string)).getOrElse(parseWithoutPattern(string))
      result <- valueWithinBounds(parsed)
    } yield result
  }

  def format(value: N): String = {
    if (pattern.isDefault) {
      formatWithoutPattern(value)
    } else {
      formatUsingPattern(value)
    }
  }

  protected def outOfBoundsException(originalInput: String): NumericParserException = {
    new NumericParserException(s"The number '$originalInput' is out of range <$min, $max>")
  }

  protected def valueWithinBounds(value: N): Try[N] = {
    valueWithinBounds(value, value.toString)
  }

  protected def valueWithinBounds(value: N, originalInput: String): Try[N] = {
    // to be abe to use comparison on N type
    val ordering = implicitly[Ordering[N]]
    import ordering._

    (this.min, this.max) match { // using this because of ambiguity with imports from ordering
      case (Some(minDefined), _) if value < minDefined => Failure(outOfBoundsException(originalInput))
      case (_, Some(maxDefined)) if value > maxDefined => Failure(outOfBoundsException(originalInput))
      case _                                           => Success(value)
    }
  }

  protected def parseWithoutPattern(stringToParse: String):Try[N] = {
    Try(stringConversion(normalizeBasicSymbols(stringToParse)))
  }

  protected def formatWithoutPattern(value: N): String = {
    denormalizeBasicSymbols(value.toString)
  }

  protected def normalizeBasicSymbols(string: String): String = {
    import za.co.absa.enceladus.utils.implicits.StringImplicits.StringEnhancements
    val replacements = NumericParser.defaultDecimalSymbols.basicSymbolsDifference(pattern.decimalSymbols)
    if (replacements.nonEmpty) {
      string.replaceChars(replacements)
    } else {
      string
    }
  }

  protected def denormalizeBasicSymbols(string: String): String = {
    import za.co.absa.enceladus.utils.implicits.StringImplicits.StringEnhancements
    val replacements = pattern.decimalSymbols.basicSymbolsDifference(NumericParser.defaultDecimalSymbols)
    if (replacements.nonEmpty) {
      string.replaceChars(replacements)
    } else {
      string
    }
  }
}

object NumericParser {
  val defaultDecimalSymbols: DecimalSymbols = DecimalSymbols(Locale.US)

  class NumericParserException(s: String = "") extends NumberFormatException(s)
}
