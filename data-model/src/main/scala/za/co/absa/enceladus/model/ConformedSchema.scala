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

import org.apache.spark.sql.types.StructField
import za.co.absa.enceladus.model.conformanceRule._

case class ConformedSchema(schema: List[StructField], dataset: Dataset) {
  def hasField(field: String): Boolean = {
    if (schema.exists(_.name == field)) true else {
      val ss = dataset.conformance.find {
        case MappingConformanceRule(_, _, _, _, _, _, outputColumn, additionalColumns, _, _, _) =>
          outputColumn == field || additionalColumns.getOrElse(Map()).contains(field)
        case SingleColumnConformanceRule(_, _, outputColumn, _, inputColumnAlias) =>
          outputColumn == field || field == outputColumn + "." + inputColumnAlias
        case DropConformanceRule(_, _, _) => false
        case c: ConformanceRule => c.outputColumn == field
      }

      ss match {
        case None => false
        case Some(matchedRule: ConformanceRule) =>
          val maybeRule = dataset.conformance.find {
            case DropConformanceRule(_, _, outputCol) => outputCol == matchedRule.outputColumn
            case _ => false
          }
          maybeRule.isEmpty
        }
    }
  }
}


