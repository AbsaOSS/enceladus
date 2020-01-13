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

package za.co.absa.enceladus.utils.validation.field

import java.util.Date
import java.sql.Timestamp
import java.util.TimeZone

import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue}
import za.co.absa.enceladus.utils.time.DateTimePattern
import za.co.absa.enceladus.utils.types.TypedStructField
import za.co.absa.enceladus.utils.types.TypedStructField.DateTimeTypeStructField
import za.co.absa.enceladus.utils.types.parsers.DateTimeParser

import scala.util.control.NonFatal

abstract class DateTimeFieldValidator extends FieldValidator {
  override def validate(field: TypedStructField): Seq[ValidationIssue] = {
    super.validate(field) ++ (
    field match {
      case dateTimeField: DateTimeTypeStructField[_] => validateDateTimeTypeStructField(dateTimeField)
      case _ => Seq(ValidationError("DateTimeFieldValidator can validate only fields of type Date or Timestamp"))
    })
  }

  private def validateDateTimeTypeStructField(field: DateTimeTypeStructField[_]): Seq[ValidationIssue] = {
    val result = for {
      parser <- field.parser
      defaultValue: Option[String] = field.getMetadataString(MetadataKeys.DefaultValue)
      defaultTimeZone: Option[String] = field.getMetadataString(MetadataKeys.DefaultTimeZone)
    } yield patternConversionIssues(field, parser).toSeq ++
      defaultTimeZoneIssues(defaultTimeZone) ++
      patternAnalysisIssues(parser.pattern, defaultValue, defaultTimeZone)

    tryToValidationIssues(result)
  }

  private def patternConversionIssues(field: DateTimeTypeStructField[_], parser: DateTimeParser): Option[ValidationIssue] = {

    try {
      implicit val implicitParser: DateTimeParser = parser

      field.ownDefaultValue.get //if Failure will throw the exception

      val exampleDateStr = parser.format(DateTimeFieldValidator.exampleDate)
      verifyStringDateTime(exampleDateStr)
      val epochStartStr = parser.format(DateTimeFieldValidator.epochStart)
      verifyStringDateTime(epochStartStr)
      val epochStartDayEndStr = parser.format(DateTimeFieldValidator.epochStartDayEnd)
      verifyStringDateTime(epochStartDayEndStr)

      None
    }
    catch {
      case NonFatal(e) => Option(ValidationError(e.getMessage))
    }
  }

  private def defaultTimeZoneIssues(defaultTimeZone: Option[String]): Seq[ValidationIssue] = {
    defaultTimeZone.filterNot(TimeZone.getAvailableIDs().contains(_)).map(tz =>
      ValidationError(""""%s" is not a valid time zone designation""".format(tz))
    ).toSeq
  }

  protected def patternAnalysisIssues(pattern: DateTimePattern,
                                      defaultValue: Option[String],
                                      defaultTimeZone: Option[String]): Seq[ValidationIssue]

  protected def verifyStringDateTime(dateTime: String)(implicit parser: DateTimeParser): Date
}

object DateTimeFieldValidator {
  private val dayMilliSeconds = 24 * 60 * 60 * 1000
  private val exampleDate = new Timestamp(System.currentTimeMillis)
  private val epochStart = new Timestamp(0)
  private val epochStartDayEnd = new Timestamp(dayMilliSeconds - 1)
}

