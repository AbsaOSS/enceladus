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

package za.co.absa.enceladus.model.test

import java.time.ZoneOffset

import org.scalatest.matchers.{MatchResult, Matcher}
import za.co.absa.enceladus.model.{Dataset, MappingTable, Schema}

trait VersionedModelMatchers {

  def matchTo(dataset: Dataset): DatasetMatcher = DatasetMatcher(dataset)
  def matchTo(schema: Schema): SchemaMatcher = SchemaMatcher(schema)
  def matchTo(mappingTable: MappingTable): MappingTableMatcher = MappingTableMatcher(mappingTable)

  case class DatasetMatcher(right: Dataset) extends Matcher[Dataset] {
    override def apply(left: Dataset): MatchResult = {
      val leftInUTC = left.copy(
        dateCreated = left.dateCreated.withZoneSameInstant(ZoneOffset.UTC),
        lastUpdated = left.lastUpdated.withZoneSameInstant(ZoneOffset.UTC),
        dateDisabled = left.dateDisabled.map(_.withZoneSameInstant(ZoneOffset.UTC))
      )
      val rightInUTC = right.copy(
        dateCreated = right.dateCreated.withZoneSameInstant(ZoneOffset.UTC),
        lastUpdated = right.lastUpdated.withZoneSameInstant(ZoneOffset.UTC),
        dateDisabled = right.dateDisabled.map(_.withZoneSameInstant(ZoneOffset.UTC))
      )
      MatchResult(leftInUTC == rightInUTC, s"$left was not equal to $right", s"$left was equal to $right")
    }
  }

  case class SchemaMatcher(right: Schema) extends Matcher[Schema] {
    override def apply(left: Schema): MatchResult = {
      val leftInUTC = left.copy(
        dateCreated = left.dateCreated.withZoneSameInstant(ZoneOffset.UTC),
        lastUpdated = left.lastUpdated.withZoneSameInstant(ZoneOffset.UTC),
        dateDisabled = left.dateDisabled.map(_.withZoneSameInstant(ZoneOffset.UTC))
      )
      val rightInUTC = right.copy(
        dateCreated = right.dateCreated.withZoneSameInstant(ZoneOffset.UTC),
        lastUpdated = right.lastUpdated.withZoneSameInstant(ZoneOffset.UTC),
        dateDisabled = right.dateDisabled.map(_.withZoneSameInstant(ZoneOffset.UTC))
      )
      MatchResult(leftInUTC == rightInUTC, s"$left was not equal to $right", s"$left was equal to $right")
    }
  }

  case class MappingTableMatcher(right: MappingTable) extends Matcher[MappingTable] {
    override def apply(left: MappingTable): MatchResult = {
      val leftInUTC = left.copy(
        dateCreated = left.dateCreated.withZoneSameInstant(ZoneOffset.UTC),
        lastUpdated = left.lastUpdated.withZoneSameInstant(ZoneOffset.UTC),
        dateDisabled = left.dateDisabled.map(_.withZoneSameInstant(ZoneOffset.UTC))
      )
      val rightInUTC = right.copy(
        dateCreated = right.dateCreated.withZoneSameInstant(ZoneOffset.UTC),
        lastUpdated = right.lastUpdated.withZoneSameInstant(ZoneOffset.UTC),
        dateDisabled = right.dateDisabled.map(_.withZoneSameInstant(ZoneOffset.UTC))
      )
      MatchResult(leftInUTC == rightInUTC, s"$left was not equal to $right", s"$left was equal to $right")
    }
  }

}
