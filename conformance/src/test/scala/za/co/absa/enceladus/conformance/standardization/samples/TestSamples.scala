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

package za.co.absa.enceladus.conformance.standardization.samples

import java.sql.Timestamp
import java.sql.Date
import java.math.BigDecimal
import za.co.absa.enceladus.utils.error.ErrorMessage
import java.time.LocalDate
import java.util.TimeZone
import java.util.Calendar

case class EmployeeNumber(numberType : String, numbers: Seq[String])
case class EmployeeNumberStd(numberType : String, numbers: Seq[Int])
case class Employee(name: String, surname: Option[String], hoursWorked: List[String], employeeNumbers: List[EmployeeNumber] = List(), startDate: String, updated: Option[Double] = None)
case class StdEmployee(name: String, surname: String, hoursWorked: Option[List[Int]], employeeNumbers: List[EmployeeNumberStd] = List(), startDate: java.sql.Date, updated: Option[Timestamp] = None, errCol: List[ErrorMessage])

case class Leg(
  accruedInterestTxnCcy: BigDecimal,
  carry:                 Option[BigDecimal] = None, cashRepCcy: Option[BigDecimal] = None, cashTxnCcy: Option[BigDecimal] = None, currencyName: String = null, currentRate: Option[BigDecimal] = None,
  currentSpread: Option[BigDecimal] = None, dayCountMethod: String = null, endDate: Date = null, faceValueRepCcy: Option[BigDecimal] = None, faceValueTxnCcy: Option[BigDecimal] = None,
  fixedRate: Option[BigDecimal] = None, floatRateSpread: Option[BigDecimal] = None, isPayLeg: String = null, floatRateReferenceName: String = null,
  legFloatRateFactor: Option[Long] = None, legNumber: String = null, legStartDate: Date = null, legType: String = null, nominalRepCcy: Option[BigDecimal] = None,
  nominalTxnCcy: Option[BigDecimal] = None, price: String = null, pvRepCcy: Option[BigDecimal] = None, pvTxnCcy: Option[BigDecimal] = None, repoRate: String = null, rollingPeriod: String = null)

case class Leg1(leg: Seq[Leg])

case class StdTradeInstrument(
  batchId: Option[Long] = None, requestId: String = null,
  contractSize: Option[BigDecimal] = None, currencyName: String = null,
  digital: Option[Boolean] = None, endDateTime: Timestamp = null,
  expiryDate: Date = null, expiryTime: Timestamp = null, fxOptionType: String = null,
  instrumentAddress: String = null, instrumentName: String = null, instrumentType: String = null,
  isExpired: Option[Boolean] = null,
  legs:      Leg1            = null, optionExoticType: String = null, otc: Option[Boolean] = None,
  payDayOffset: Option[Long] = None, payOffsetMethod: String = null, payType: String = null, quoteType: String = null,
  realDividendValue: String = null, refValue: Option[Long] = None, referencePrice: Option[BigDecimal] = None, reportDate: Date = null,
  settlementType: String = null, spotBankingDayOffset: Option[Long] = None,
  startDate: Date = null, strikeCurrencyName: String = null, strikeCurrencyNumber: Option[Long] = None, tradeId: String = null, tradeNumber: String = null,
  txnMaturityPeriod: String = null, underlyingInstruments: String = null, valuationGroupName: String = null,
  versionId: String = null, errCol: Seq[ErrorMessage] = Seq())

case class DateTimestampData(
  id:            Long,
  dateSampleOk1: String, dateSampleOk2: String, dateSampleOk3: String,
  dateSampleWrong1: String, dateSampleWrong2: String, dateSampleWrong3: String,
  timestampSampleOk1: String, timestampSampleOk2: String, timestampSampleOk3: String,
  timestampSampleWrong1: String, timestampSampleWrong2: String, timestampSampleWrong3: String)

object TestSamples {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  val john0 = Employee(name = "John0",surname = None, hoursWorked = List("8", "7", "8", "9", "12", null), employeeNumbers = List(EmployeeNumber("SAP", List("456", "123")), EmployeeNumber("WD", List("00005"))), startDate = "2015-08-01")
  val john1 = Employee(name = "John1", surname = Some("Doe1"), hoursWorked = List("99", "99", "76", "12", "12", "24"), startDate = "Two Thousand Something")
  val john2 = Employee(name = "John2", surname = None, hoursWorked = null, startDate = "2015-08-01", updated = Some(20150716.133224))
  val john3 = Employee(name = "John3", surname = None, hoursWorked = List(), startDate = "2015-08-01", updated = Some(20150716.103224))

  val data1 = List(john0, john1, john2, john3)

  val startDate = 1438387200000l //01/08/2015

  val resData = List(
     StdEmployee(name = "John0", surname = "Unknown Surname", hoursWorked = Some(List(8, 7, 8, 9, 12, 0)),
        employeeNumbers = List(EmployeeNumberStd("SAP", List(456, 123)), EmployeeNumberStd("WD", List(5))), startDate =  new java.sql.Date(startDate), errCol = List(ErrorMessage.stdNullErr("surname"), ErrorMessage.stdNullErr("hoursWorked[*]"))),
     StdEmployee(name = "John1", surname = "Doe1", hoursWorked = Some(List(99, 99, 76, 12, 12, 24)), startDate = new java.sql.Date(0), errCol = List(ErrorMessage.stdCastErr("startDate", "Two Thousand Something"))),
     StdEmployee(name = "John2", surname = "Unknown Surname", hoursWorked = None, startDate = new java.sql.Date(startDate), updated = Some(Timestamp.valueOf("2015-07-16 13:32:24")), errCol = List(ErrorMessage.stdNullErr("surname"), ErrorMessage.stdNullErr("hoursWorked"))),
     StdEmployee(name = "John3", surname = "Unknown Surname", hoursWorked = Some(List()), startDate = new java.sql.Date(startDate), updated = Some(Timestamp.valueOf("2015-07-16 10:32:24")), errCol = List(ErrorMessage.stdNullErr("surname"))))

  val dateSamples = List(DateTimestampData(
    1,
    "20-10-2017", "2017-10-20", "12/29/2017",
    "10-20-2017", "201711", "",
    "2017-10-20T08:11:31", "2017-10-20_08:11:31", "20171020081131",
    "20171020T081131", "2017-10-20t081131", "2017-10-20"))
}
