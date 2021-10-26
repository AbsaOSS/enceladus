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

package za.co.absa.enceladus.utils.library.sizer

import org.apache.spark.sql.types.DataType

case class DataTypeSizes(typeSizes: Map[DataType, ByteSize], averageArraySize: Int) {
  def apply(dataType: DataType): ByteSize = {
    typeSizes.getOrElse(dataType, 0)
  }

  def withDataTypeSize(dataType: DataType, typeSize: ByteSize): DataTypeSizes = {
    val newMap = typeSizes + (dataType -> typeSize)
    copy(typeSizes = newMap)
  }
}

object DataTypeSizes {
  final val DefaultDataTypeSizes = new DataTypeSizes(Map.empty, 0) //TODO some reasonable default, perhaps one optionaly read from config file
}


