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

package za.co.absa.enceladus.utils.error

object ErrorMessageFactory {
  val errColSchema: String =  "\n |-- errCol: array (nullable = true)\n"+
    " |    |-- element: struct (containsNull = false)\n"+
    " |    |    |-- errType: string (nullable = true)\n"+
    " |    |    |-- errCode: string (nullable = true)\n"+
    " |    |    |-- errMsg: string (nullable = true)\n"+
    " |    |    |-- errCol: string (nullable = true)\n"+
    " |    |    |-- rawValues: array (nullable = true)\n"+
    " |    |    |    |-- element: string (containsNull = true)\n"+
    " |    |    |-- mappings: array (nullable = true)\n"+
    " |    |    |    |-- element: struct (containsNull = true)\n"+
    " |    |    |    |    |-- mappingTableColumn: string (nullable = true)\n"+
    " |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)\n"
}
