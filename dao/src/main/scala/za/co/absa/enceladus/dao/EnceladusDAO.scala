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

package za.co.absa.enceladus.dao

import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.MappingTable
import org.apache.spark.sql.types.StructType

trait EnceladusDAO {
  def getDataset(name: String, version: Int) : Dataset
  def getMappingTable(name: String, version: Int): MappingTable
  def getSchema(name: String, version: Int): StructType
}