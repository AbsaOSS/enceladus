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

package za.co.absa.enceladus.utils.types.parsers

import java.text.DecimalFormat

import za.co.absa.enceladus.utils.numeric.NumericPattern
import za.co.absa.enceladus.utils.typeClasses.DoubleLike
import za.co.absa.enceladus.utils.types.parsers.NumericParser.NumericParserException

class FractionalParser[D: DoubleLike] private(override val pattern: NumericPattern,
                                              override val min: Option[D],
                                              override val max: Option[D])
  extends NumericParser(pattern, min, max) with ParseViaDecimalFormat[D] {

  private val ev = implicitly[DoubleLike[D]]

  override protected val stringConversion: String => D = stringToDWithoutPattern
  override protected val numberConversion: Number => D = {number => ev.toT(number.doubleValue())}

  protected val decimalFormat: Option[DecimalFormat] = pattern
    .specifiedPattern
    .map(new DecimalFormat(_, pattern.decimalSymbols.toDecimalFormatSymbols))

  private def stringToDWithoutPattern(string: String): D = {
    val resultAsDouble = string match {
      case pattern.decimalSymbols.naNValue => Double.NaN
      case pattern.decimalSymbols.infinityValue => Double.PositiveInfinity
      case pattern.decimalSymbols.negativeInfinityValue => Double.NegativeInfinity
      case _ => string.toDouble
    }
    ev.toT(resultAsDouble)
  }
}

object FractionalParser {
  def apply(pattern: NumericPattern,
            min: Double = Double.MinValue,
            max: Double = Double.MaxValue): FractionalParser[Double] = {
    new FractionalParser(pattern, Option(min), Option(max))
  }

  def apply[D: DoubleLike](pattern: NumericPattern,
                           min: D,
                           max: D): FractionalParser[D] = {
    new FractionalParser[D](pattern, Option(min), Option(max))
  }

  def withInfinity[D: DoubleLike](pattern: NumericPattern): FractionalParser[D] = {
    val ev = implicitly[DoubleLike[D]]
    val negativeInfinity = ev.toT(Double.NegativeInfinity)
    val positiveInfinity = ev.toT(Double.PositiveInfinity)
    new FractionalParser(pattern, Option(negativeInfinity), Option(positiveInfinity))
  }
}
