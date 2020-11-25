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

package za.co.absa.enceladus.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonSubTypes, JsonTypeInfo}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

package object dataFrameFilter {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
  @JsonSubTypes(Array(
    new Type(value = classOf[OrJoinedFilters], name = "OrJoinedFilters"),
    new Type(value = classOf[AndJoinedFilters], name = "AndJoinedFilters"),
    new Type(value = classOf[EqualFilter], name = "EqualFilter"),
    new Type(value = classOf[DifferFilter], name = "DifferFilter")
  ))
  sealed trait DataFrameFilter {
    @JsonIgnore def filter: Column
    @JsonIgnore def or(filter: DataFrameFilter): DataFrameFilter = {
      (this, filter) match {
        case (a: OrJoinedFilters, b: OrJoinedFilters) => OrJoinedFilters(a.filterItems & b.filterItems)
        case (a: OrJoinedFilters, b) => a.copy(filterItems = a.filterItems + b)
        case (a, b: OrJoinedFilters) => b.copy(filterItems = b.filterItems + a)
        case (a, b) => OrJoinedFilters(Set(a, b))
      }
    }

    @JsonIgnore def and(filter: DataFrameFilter): DataFrameFilter = {
      (this, filter) match {
        case (a: AndJoinedFilters, b: AndJoinedFilters) => AndJoinedFilters(a.filterItems & b.filterItems)
        case (a: AndJoinedFilters, b) => a.copy(filterItems = a.filterItems + b)
        case (a, b: AndJoinedFilters) => b.copy(filterItems = b.filterItems + a)
        case (a, b) => AndJoinedFilters(Set(a, b))
      }
    }

    def + (filter: DataFrameFilter): DataFrameFilter = or(filter) //scalastyle:ignore class.name function used as operator
    def *(filter: DataFrameFilter): DataFrameFilter = and(filter) //scalastyle:ignore class.name function used as operator

  }

  sealed trait JoinFilters extends DataFrameFilter {
    @JsonIgnore protected def operator: (Column, Column) => Column

    def filterItems: Set[DataFrameFilter]
    def filter: Column = {
      if (filterItems.isEmpty) {
        lit(true)
      } else {
        filterItems.tail.foldLeft(filterItems.head.filter) { case(col, filterDef) =>
          operator(col, filterDef.filter)
        }
      }
    }
  }

  sealed trait SingleColumnAndValueFilter extends DataFrameFilter {
    def columnName: String
    def value: Any

    @JsonIgnore protected def operator: (Column, Any) => Column
    override def filter: Column = {
      operator(col(columnName), value)
    }
  }

  case class OrJoinedFilters(filterItems: Set[DataFrameFilter]) extends JoinFilters {
    protected val operator: (Column, Column) => Column = (a: Column, b: Column) => { a or b }
  }

  case class AndJoinedFilters(filterItems: Set[DataFrameFilter]) extends JoinFilters {
    protected val operator: (Column, Column) => Column = (a: Column, b: Column) => { a and b }
  }

  case class EqualFilter(columnName: String, value: Any) extends SingleColumnAndValueFilter {
    protected val operator: (Column, Any) => Column = (column: Column, value: Any) => { column === value }
  }

  case class DifferFilter(columnName: String, value: Any) extends SingleColumnAndValueFilter {
    protected val operator: (Column, Any) => Column = (column: Column, value: Any) => { column =!= value }
  }

}
