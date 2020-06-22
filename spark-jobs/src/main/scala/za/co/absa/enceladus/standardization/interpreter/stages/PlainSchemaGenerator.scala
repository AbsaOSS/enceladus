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

package za.co.absa.enceladus.standardization.interpreter.stages

import org.apache.spark.sql.types._
import za.co.absa.enceladus.utils.schema.MetadataKeys

/**
 * This component is used in the standardization job. We've got a strongly typed (target) schema. When reading the data however, we do not want spark to apply casts
 * automatically. Instead we want to read all primitive types as strings. This component takes the target (desired) schema and generates a plain one which is used for
 * reading source data in the job.
 */
object PlainSchemaGenerator {

  private def structTypeFieldsConversion(fields: Array[StructField]):  Array[StructField] = {
    import za.co.absa.enceladus.utils.implicits.StructFieldImplicits.StructFieldEnhancements
    fields.map { field =>
      // If the meta data value sourcecolumn is set override the field name
      val fieldName = field.getMetadataString(MetadataKeys.SourceColumn).getOrElse(field.name)
      val dataType = inputSchemaAsStringTypes(field.dataType)
      StructField(fieldName, dataType, nullable = true, field.metadata) //metadata not needed to be transferred
    }
  }

  private def inputSchemaAsStringTypes(inp: DataType): DataType = {
    inp match {
      case st: StructType => StructType(structTypeFieldsConversion(st.fields))
      case at: ArrayType  => ArrayType(inputSchemaAsStringTypes(at.elementType), containsNull = true)
      case _: DataType    => StringType
    }
  }

  def generateInputSchema(structType: StructType, corruptRecordFieldName: Option[String] = None): StructType = {
    val inputSchema = structTypeFieldsConversion(structType.fields)
    val corruptRecordField = corruptRecordFieldName.map(StructField(_, StringType)).toArray
    StructType(inputSchema ++ corruptRecordField)
  }


}
