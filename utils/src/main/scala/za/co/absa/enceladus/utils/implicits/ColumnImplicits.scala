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

    /**
      * Spark strings are base on 1 unlike scala. The function shifts the substring indexation to be in accordance with
      * Scala/ Java.
      * Another enhancement is, that the function allows a negative index, denoting counting of the index from back
      * This version takes the substring from the startPos until the end.
      * @param startPos the index (zero based) where to start the substring from, if negative it's counted from end
      * @return         column with requested substring
      */
    def zeroBasedSubstr(startPos: Int): Column = {
      if (startPos >= 0) {
        zeroBasedSubstr(startPos, Int.MaxValue - startPos)
      } else {
        zeroBasedSubstr(startPos, -startPos)
      }
    }

    /**
      * Spark strings are base on 1 unlike scala. The function shifts the substring indexation to be in accordance with
      * Scala/ Java.
      * Another enhancement is, that the function allows a negative index, denoting counting of the index from back
      * This version takes the substring from the startPos and takes up to the given number of characters (less.
      * @param startPos the index (zero based) where to start the substring from, if negative it's counted from end
      * @param len      length of the desired substring, if longer then the rest of the string, all the remaining characters are taken
      * @return         column with requested substring
      */
    def zeroBasedSubstr(startPos: Int, len: Int): Column = {
      if (startPos >= 0) {
        column.substr(startPos + 1, len)
      } else {
        val startPosColumn = greatest(length(column) + startPos + 1, lit(1))
        val lenColumn = lit(len) + when(length(column) + startPos <= 0, length(column) + startPos).otherwise(0)
        column.substr(startPosColumn,lenColumn)
      }
    }

    /**
      * Spark strings are base on 1 unlike scala. The function shifts the substring indexation to be in accordance with
      * Scala/ Java.
      * Another enhancement is, that the function allows a negative index, denoting counting of the index from back
      * This version takes the startPos and len from the provided Section object.
      * @param section  the start and requested length of the substring encoded within the Section object
      * @return         column with requested substring
      */
    def zeroBasedSubstr(section: Section): Column = zeroBasedSubstr(section.start, section.length)

    /**
      * Removes part of a StringType column, defined by the provided section. A column containing the remaining part of
      * the string is returned
      * @param section  Definition of the part of the string to remove
      * @return         Column with the remaining parts of the string (concatenated)
      */
    def removeSection(section: Section): Column = {
      splitBySection(section) match {
        case Left(result) => result
        case Right((leftColumn, rightColumn)) => concat(leftColumn, rightColumn)
      }
    }

    /**
      * Removes multiple sections from a StringType column. The operation is done in a way, as if the sections would be
      * removed all "at once". E.g. removing Section(3, 1) and Section(6,1) removes the 3rd and 6th character (zero based),
      * NOT the 3rd and 7th.
      * @param sections Sections to removed
      * @return         Column with the remainders of the string concatenated
      */
    def removeSections(sections: Seq[Section]): Column = {
      val mergedSections = Section.mergeTouchingSectionsAndSort(sections)
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
