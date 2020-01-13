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

package za.co.absa.enceladus.utils.numeric

import java.text.DecimalFormatSymbols
import java.util.Locale

/**
  * It's an immutable wrapper of Java's DecimalFormatSymbols
  * @param decimalSeparator   the character used for decimal sign
  * @param groupingSeparator  the character used for thousands separator
  * @param minusSign          the character used to represent minus sign
  * @param patternSeparator   the character used to separate positive and negative subpatterns in a pattern
  * @param percentSign        the character used for percent sign
  * @param permillSign        the character used for per mille sign
  * @param exponentSeparator  the string used to separate the mantissa from the exponent
  * @param infinityValue      the string used to represent infinity
  * @param naNValue           the string used to represent "not a number"
  */
case class DecimalSymbols(
                           decimalSeparator: Char,
                           groupingSeparator: Char,
                           minusSign: Char,
                           patternSeparator: Char,
                           percentSign: Char,
                           permillSign: Char,
                           exponentSeparator: String,
                           infinityValue: String,
                           naNValue: String) {
  val negativeInfinityValue = s"$minusSign$infinityValue"

  def toDecimalFormatSymbols: DecimalFormatSymbols = {
    val result = new DecimalFormatSymbols(Locale.US)
    result.setDecimalSeparator(decimalSeparator)
    result.setGroupingSeparator(groupingSeparator)
    result.setMinusSign(minusSign)
    result.setPatternSeparator(patternSeparator)
    result.setPercent(percentSign)
    result.setPerMill(permillSign)
    result.setExponentSeparator(exponentSeparator)
    result.setInfinity(infinityValue)
    result.setNaN(naNValue)
    result
  }

  def charSymbolsDifference(from: DecimalSymbols): Map[Char, Char] = {
    Map(
      from.decimalSeparator -> decimalSeparator,
      from.minusSign -> minusSign,
      from.patternSeparator -> patternSeparator,
      from.percentSign -> percentSign,
      from.permillSign -> permillSign,
      from.groupingSeparator -> groupingSeparator
    ).filter(charsDiffer)
  }

  def basicSymbolsDifference(from: DecimalSymbols): Map[Char, Char] = {
    Map(
      from.decimalSeparator -> decimalSeparator,
      from.minusSign -> minusSign,
      // replace the standard symbols to "invalidate" them
      decimalSeparator -> from.decimalSeparator,
      minusSign -> from.minusSign
    ).filter(charsDiffer)
  }

  private def charsDiffer(chars: (Char, Char)): Boolean = {
    chars._1 != chars._2
  }

}

object DecimalSymbols {
  def apply(locale: Locale): DecimalSymbols = {
    DecimalSymbols(new DecimalFormatSymbols(locale))
  }

  def apply(dfs: DecimalFormatSymbols): DecimalSymbols = {
    DecimalSymbols(
      decimalSeparator = dfs.getDecimalSeparator,
      minusSign = dfs.getMinusSign,
      patternSeparator = dfs.getPatternSeparator,
      percentSign = dfs.getPercent,
      permillSign = dfs.getPerMill,
      infinityValue = dfs.getInfinity,
      exponentSeparator = dfs.getExponentSeparator,
      naNValue = dfs.getNaN,
      groupingSeparator = dfs.getGroupingSeparator
    )
  }
}
