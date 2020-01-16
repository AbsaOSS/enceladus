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

import java.math.BigInteger
import java.text.DecimalFormat

import scala.util.{Failure, Success, Try}
import za.co.absa.enceladus.utils.numeric.{DecimalSymbols, NumericPattern, Radix}
import za.co.absa.enceladus.utils.typeClasses.LongLike

abstract class IntegralParser[N: LongLike] private(override val pattern: NumericPattern,
                                                   override val min: Option[N],
                                                   override val max: Option[N])
  extends NumericParser[N](pattern, min, max) {
  protected val ev: LongLike[N] = implicitly[LongLike[N]]

  val radix: Radix

  override protected val stringConversion: String => N = {string => ev.stringToT(string)}
}

object IntegralParser {
  def apply(pattern: NumericPattern,
            min: Long = Long.MinValue,
            max: Long = Long.MaxValue): IntegralParser[Long] = {
    new PatternIntegralParser(pattern, Option(min), Option(max))
  }

  def apply[N: LongLike](pattern: NumericPattern,
                         min: Option[N],
                         max: Option[N]): IntegralParser[N] = {
    new PatternIntegralParser(pattern, min, max)
  }

  def ofRadix(radix: Radix,
              decimalSymbols: DecimalSymbols = NumericParser.defaultDecimalSymbols,
              min: Long = Long.MinValue,
              max: Long = Long.MaxValue): IntegralParser[Long] = {
    new RadixIntegralParser(radix, decimalSymbols, Option(min), Option(max))
  }

  def ofRadix[N: LongLike](radix: Radix,
                           decimalSymbols: DecimalSymbols,
                           min: Option[N],
                           max: Option[N]): IntegralParser[N] = {
    new RadixIntegralParser[N](radix, decimalSymbols, min, max)
  }

  def ofStringRadix(stringRadix: String,
                    decimalSymbols: DecimalSymbols = NumericParser.defaultDecimalSymbols,
                    min: Long = Long.MinValue,
                    max: Long = Long.MaxValue): IntegralParser[Long] = {
    ofRadix(Radix(stringRadix), decimalSymbols, min, max)
  }

  def ofStringRadix[N: LongLike](stringRadix: String,
                                 decimalSymbols: DecimalSymbols,
                                 min: Option[N],
                                 max: Option[N]): IntegralParser[N] = {
    ofRadix(Radix(stringRadix), decimalSymbols, min, max)
  }

  def tryStringToBase(string: String): Try[Radix] = {
    Try(Radix(string))
  }

  final class RadixIntegralParser[N: LongLike] (override val radix: Radix,
                                                decimalSymbols: DecimalSymbols,
                                                override val min: Option[N],
                                                override val max: Option[N])
    extends IntegralParser(NumericPattern(decimalSymbols), min, max) {

    private val minBI = BigInteger.valueOf(min.map(ev.toLong).getOrElse(Long.MinValue))
    private val maxBI = BigInteger.valueOf(max.map(ev.toLong).getOrElse(Long.MaxValue))

    override def parse(string: String): Try[N] = {
      val preprocessed = normalizeBasicSymbols(string)
      radix match {
        case Radix.DefaultRadix => Try{ev.stringToT(preprocessed)}.flatMap(valueWithinBounds(_, string))
        case Radix(16) => toNWithBoundCheck(clearHexString(preprocessed), string)// scalastyle:ignore magic.number obvious meaning
        case _ => toNWithBoundCheck(preprocessed, string)
      }
    }

    override def format(value: N): String = {
      // scalastyle:off magic.number obvious meaning
      val longValue = ev.toLong(value)
      val result = radix match {
        case Radix(10) => longValue.toString
        case Radix(16) => longValue.toHexString
        case Radix(2)  => longValue.toBinaryString
        case Radix(8)  => longValue.toOctalString
        case Radix(b)  =>
          val bigValue = BigInteger.valueOf(longValue)
          bigValue.toString(b)
      }
      // scalastyle:on magic.number
      denormalizeBasicSymbols(result)
    }

    private def toNWithBoundCheck(string: String, originalInput: String): Try[N] = {
      val bigIntegerTry = Try{new BigInteger(string, radix.value)}
      bigIntegerTry.flatMap(bigValue => {
        if (bigValue.compareTo(maxBI) > 0) { // too big
          Failure(outOfBoundsException(originalInput))
        } else if (bigValue.compareTo(minBI) < 0) { // too small
          Failure(outOfBoundsException(originalInput))
        } else {
          Success(ev.toT(bigValue.longValue()))
        }
      })
    }

    private def clearHexString(string: String): String = {
      // supporting 0xFF style format of hexadecimals
      val longEnoughString = string + "   "
      val prefix2 = longEnoughString.substring(0, 2).toLowerCase
      if (prefix2 == "0x") {
        string.substring(2)
      } else {
        val prefix3 = longEnoughString.substring(0, 3).toLowerCase
        if (prefix3 == "-0x") {
          "-" + string.substring(3)
        } else if (prefix3 == "+0x") {
          string.substring(3)
        } else {
          string
        }
      }
    }

    protected def parseUsingPattern(stringToParse: String):Try[N] = parse(stringToParse)
    protected def formatUsingPattern(value: N): String = format(value)

  }

  final class PatternIntegralParser[N: LongLike](override val pattern: NumericPattern,
                                                 override val min: Option[N],
                                                 override val max: Option[N])
    extends IntegralParser(pattern, min, max) with ParseViaDecimalFormat[N] {
    override val radix: Radix = Radix.DefaultRadix

    override protected val numberConversion: Number => N = {number =>
      val longValue = number.longValue()
      if ((longValue > ev.MaxValue) || (longValue < ev.MinValue)) {
        throw outOfBoundsException(number.toString)
      }
      ev.toT(longValue)
    }
    override protected val decimalFormat: Option[DecimalFormat] = pattern.specifiedPattern.map(s => {
      val df = new DecimalFormat(s, pattern.decimalSymbols.toDecimalFormatSymbols)
      df.setParseIntegerOnly(true)
      df
    })
  }
}
