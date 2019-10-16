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

import java.text.DecimalFormat
import za.co.absa.enceladus.utils.numeric.NumericPattern

class DecimalParser(override val pattern: NumericPattern,
                    override val min: Option[BigDecimal],
                    override val max: Option[BigDecimal])
  extends NumericParser(pattern, min, max) with ParseViaDecimalFormat[BigDecimal] {

  override protected val stringConversion: String => BigDecimal = BigDecimal(_)
  override protected val numberConversion: Number => BigDecimal = {n => BigDecimal(n.asInstanceOf[java.math.BigDecimal])}

  protected val decimalFormat: Option[DecimalFormat] = pattern.specifiedPattern.map (s => {
    val df = new DecimalFormat(s, pattern.decimalSymbols.toDecimalFormatSymbols)
    df.setParseBigDecimal(true)
    df
  })
}

object DecimalParser {
  def apply(pattern: NumericPattern,
            min: Option[BigDecimal] = None,
            max: Option[BigDecimal] = None): DecimalParser = {
    new DecimalParser(pattern, min, max)
  }
}
