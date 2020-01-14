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

package za.co.absa.enceladus.conformance.standardization.stages

import org.apache.spark.sql.types.{DataType, _}

/**
 * This component is used in the standardization job. We've got a strongly typed (target) schema. When reading the data however, we do not want spark to apply casts
 * automatically. Instead we want to read all primitive types as strings. This component takes the target (desired) schema and generates a plain one which is used for
 * reading source data in the job.
 */
object PlainSchemaGenerator {

  def generateInputSchema(inp: DataType): DataType = {
    inp match {
      case i: StructType => StructType(i.fields.map(field =>
        // If the meta data value sourcecolumn is set override the field name
        if (field.metadata.contains("sourcecolumn")) {
          StructField(name = field.metadata.getString("sourcecolumn"), nullable = true, metadata = field.metadata, dataType = generateInputSchema(field.dataType))
        } else {
          StructField(name = field.name, nullable = true, metadata = field.metadata, dataType = generateInputSchema(field.dataType))
        }
      ))
      case i: ArrayType => ArrayType.apply(generateInputSchema(i.elementType), i.containsNull)
      case _: DataType => StringType
    }
  }
}
