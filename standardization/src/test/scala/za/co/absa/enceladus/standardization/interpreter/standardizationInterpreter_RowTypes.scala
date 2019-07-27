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

package za.co.absa.enceladus.standardization.interpreter

import java.sql.{Date, Timestamp}

import za.co.absa.enceladus.utils.error.ErrorMessage

//Decimal Suite
case class DecimalRow(
                       description: String,
                       small: Option[BigDecimal],
                       big: Option[BigDecimal],
                       errCol: Seq[ErrorMessage] = Seq.empty
                     )

case class InputRowDoublesForDecimal(
                                      description: String,
                                      small: Option[Double],
                                      big: Option[Double]
                                    ) {
  def this(description: String, value: Double) = {
    this(description, Option(value), Option(value))
  }
}

//Fractional Suite
case class FractionalRow(
                          description: String,
                          floatField: Option[Float],
                          doubleField: Option[Double],
                          errCol: Seq[ErrorMessage] = Seq.empty
                        )

case class InputRowLongsForFractional(
                                       description: String,
                                       floatField: Option[Double],
                                       doubleField: Option[Double]
                                     ) {
  def this(description: String, value: Double) = {
    this(description, Option(value), Option(value))
  }
}

case class InputRowDoublesForFractional(
                                         description: String,
                                         floatField: Option[Double],
                                         doubleField: Option[Double]
                                       ) {
  def this(description: String, value: Double) = {
    this(description, Option(value), Option(value))
  }
}

//Integral Suite
case class IntegralRow(
                        description: String,
                        byteSize: Option[Byte],
                        shortSize: Option[Short],
                        integerSize: Option[Int],
                        longSize: Option[Long],
                        errCol: Seq[ErrorMessage] = Seq.empty
                      )

case class InputRowLongsForIntegral(
                                     description: String,
                                     bytesize: Long,
                                     shortsize: Long,
                                     integersize: Long,
                                     longsize: Long
                                   ) {
  def this(description: String, value: Long) = {
    this(description, value, value, value, value)
  }
}

case class InputRowDoublesForIntegral(
                                       description: String,
                                       bytesize: Option[Double],
                                       shortsize: Option[Double],
                                       integersize: Option[Double],
                                       longsize: Option[Double]
                                     ) {
  def this(description: String, value: Double) = {
    this(description, Option(value), Option(value), Option(value), Option(value))
  }
}

case class InputRowBigDecimalsForIntegral(
                                           description: String,
                                           bytesize: Option[BigDecimal],
                                           shortsize: Option[BigDecimal],
                                           integersize: Option[BigDecimal],
                                           longsize: Option[BigDecimal]
                                         ) {
  def this(description: String, value: BigDecimal) = {
    this(description, Option(value), Option(value), Option(value), Option(value))
  }
}

//Timestamp Suite
case class TimestampRow(
                         tms: Timestamp,
                         errCol: Seq[ErrorMessage] = Seq.empty
                       )

//Date Suite
case class DateRow(
                    dateField: Date,
                    errCol: Seq[ErrorMessage] = Seq.empty
                  )
