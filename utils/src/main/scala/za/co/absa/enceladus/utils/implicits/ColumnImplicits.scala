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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import za.co.absa.enceladus.utils.general.Section

object ColumnImplicits {
  implicit class ColumnEnhancements(column: Column) {
    def isInfinite: Column = {
      column.isin(Double.PositiveInfinity, Double.NegativeInfinity)
    }

    def zeroBasedSubstr(startPos: Int): Column = {
      if (startPos >= 0) {
        zeroBasedSubstr(startPos, Int.MaxValue - startPos)
      } else {
        zeroBasedSubstr(startPos, -startPos)
      }
    }

    def zeroBasedSubstr(startPos: Int, len: Int): Column = {
      if (startPos >= 0) {
        column.substr(startPos + 1, len)
      } else {
        val startPosColumn = greatest(length(column) + startPos + 1, lit(1))
        val lenColumn = lit(len) + when(length(column) + startPos <= 0, length(column) + startPos).otherwise(0)
        column.substr(startPosColumn,lenColumn)
      }
    }

    def zeroBasedSubstr(section: Section): Column = zeroBasedSubstr(section.start, section.length)

    def removeSection(section: Section): Column = {
      splitBySection(section) match {
        case Left(result) => result
        case Right((leftColumn, rightColumn)) => concat(leftColumn, rightColumn)
      }
    }

    def removeSections(sections: Seq[Section]): Column = {
      val mergedSections = Section.mergeSections(sections)
      mergedSections.foldLeft(column) ((columnAcc, item) => columnAcc.removeSection(item)) //TODO try more effectively #678
    }

    private def splitBySection(section: Section): Either[Column, (Column, Column)] = {
      def upToNegative(negativeIndex: Int): Column = column.substr(lit(1), greatest(length(column) + negativeIndex, lit(0)))

      section match {
        case Section(_, 0) => Left(column)
        case Section(0, l) => Left(zeroBasedSubstr(l))
        case Section(s, l) if (s < 0) && (s + l >= 0) => Left(upToNegative(s)) //till the end
        case Section(s, l) if s >= 0 => Right(zeroBasedSubstr(0, s), zeroBasedSubstr(s + l, Int.MaxValue))
        case Section(s, l) => Right(upToNegative(s), zeroBasedSubstr(s + l))
      }
    }

  }
}
