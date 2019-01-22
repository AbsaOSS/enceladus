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

package za.co.absa.enceladus.standardization.interpreter.stages

import java.security.InvalidParameterException
import java.util.TimeZone
import java.util.regex.Pattern

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.types.Defaults

class TypeParserSuite extends FunSuite with SparkTestBase {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private def substringCount(text: String, substr: String): Int = {
    Pattern.quote(substr).r.findAllMatchIn(text).length
  }

  def assertStringOccurances(resultToCheck: ParseOutput, substrToSearch: String, colCount: Integer, errCount: Integer = 0): Unit = {
    val resultCol = substringCount(resultToCheck.stdCol.expr.toString(), substrToSearch)
    assert(colCount == resultCol, s"Expected $colCount, got $resultCol\n  of: $substrToSearch\n  in: ${resultToCheck.stdCol.expr.toString()}")
    val resultErr = substringCount(resultToCheck.stdCol.expr.toString(), substrToSearch)
    assert(colCount == resultErr, s"Expected $colCount, got $resultCol\n  of: $substrToSearch\n  in: ${resultToCheck.errors.expr.toString()}")
  }

  test("Test standardize with sourcecolumn metadata") {
    val structFieldNoMetadata = StructField("a", IntegerType)
    val structFieldWithMetadataNotSourceColumn = StructField("b", IntegerType, nullable = false, new MetadataBuilder().putString("meta", "data").build)
    val structFieldWithMetadataSourceColumn = StructField("c", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_c").build)
    val schema = StructType(Array(structFieldNoMetadata, structFieldWithMetadataNotSourceColumn, structFieldWithMetadataSourceColumn))
    //Just Testing field name override
    import spark.implicits._
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val parseOutputStructFieldNoDefault: ParseOutput = TypeParser.standardize(structFieldNoMetadata, "path", schema)
    assertResult(true)(parseOutputStructFieldNoDefault.stdCol.expr.toString().contains("path.a"))
    assertResult(false)(parseOutputStructFieldNoDefault.stdCol.expr.toString().replaceAll("path.a", "").contains("path"))
    assertResult(true)(parseOutputStructFieldNoDefault.errors.expr.toString().contains("path.a"))
    assertResult(false)(parseOutputStructFieldNoDefault.errors.expr.toString().replaceAll("path.a", "").contains("path"))
    val parseOutputStructFieldWithMetadataNotSourceColumn = TypeParser.standardize(structFieldWithMetadataNotSourceColumn, "path", schema)
    assertResult(true)(parseOutputStructFieldWithMetadataNotSourceColumn.stdCol.expr.toString().contains("path.b"))
    assertResult(false)(parseOutputStructFieldWithMetadataNotSourceColumn.stdCol.expr.toString().replaceAll("path.b", "").contains("path"))
    assertResult(true)(parseOutputStructFieldWithMetadataNotSourceColumn.errors.expr.toString().contains("path.b"))
    assertResult(false)(parseOutputStructFieldWithMetadataNotSourceColumn.errors.expr.toString().replaceAll("path.b", "").contains("path"))
    val parseOutputStructFieldWithMetadataSourceColumn = TypeParser.standardize(structFieldWithMetadataSourceColumn, "path",schema)
    assertResult(false)(parseOutputStructFieldWithMetadataSourceColumn.stdCol.expr.toString().contains("path.c"))
    assertResult(true)(parseOutputStructFieldWithMetadataSourceColumn.stdCol.expr.toString().contains("path.override_c"))
    assertResult(false)(parseOutputStructFieldWithMetadataSourceColumn.stdCol.expr.toString().replaceAll("path.override_c", "").contains("path"))
    assertResult(false)(parseOutputStructFieldWithMetadataSourceColumn.errors.expr.toString().contains("path.c"))
    assertResult(true)(parseOutputStructFieldWithMetadataSourceColumn.errors.expr.toString().contains("path.override_c"))
    assertResult(false)(parseOutputStructFieldWithMetadataSourceColumn.errors.expr.toString().replaceAll("path.override_c", "").contains("path"))
  }


  test("Date field without format") {
    val path: String = "path"
    val defaultFormat: String = Defaults.getGlobalFormat(DateType)
    val defaultValue: String = "1999-12-31"
    val defaultValueParsed: Integer = 10956 //this is the above date transformed within the expression
    val structFieldNoDefault = StructField("field0", DateType)
    val structFieldWithDefaultMetadataColumn = StructField("field1", DateType, nullable = false, new MetadataBuilder().putString("default", defaultValue).build)
    val structFieldWithMetadataSource2Column = StructField("field2", DateType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field2_source").build)
    val structFieldWithMetadataSource3Column = StructField("field3", DateType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field3_source").build)
    val structFieldSource2Column = StructField("field2_source", DoubleType, nullable = false)
    val structFieldSource3Column = StructField("field3_source", DecimalType(10,2), nullable = false)
    val schema: StructType = StructType(Array(StructField(path, StructType(Array(structFieldNoDefault, structFieldWithDefaultMetadataColumn, structFieldWithMetadataSource2Column, structFieldWithMetadataSource3Column, structFieldSource2Column, structFieldSource3Column)))))

    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val parseOutputStructFieldNoDefault: ParseOutput = TypeParser.standardize(structFieldNoDefault, path, schema)
    assertStringOccurances(parseOutputStructFieldNoDefault, s"to_date('$path.field0, Some($defaultFormat))", 3, 1)
    val parseOutputStructFieldWithMetadataNotSourceColumn: ParseOutput = TypeParser.standardize(structFieldWithDefaultMetadataColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataNotSourceColumn, s"to_date('$path.field1, Some($defaultFormat))", 3, 1)
    assertStringOccurances(parseOutputStructFieldWithMetadataNotSourceColumn, s"ELSE $defaultValueParsed END", 1)
    val message = "Dates & times represented as numeric values need specified 'pattern' metadata"
    try {
      val parseOutputStructFieldWithMetadataSource2Column: ParseOutput = TypeParser.standardize(structFieldWithMetadataSource2Column, path, schema)
      assert(false, "Should never get here - DoubleType")
    } catch {
      case e: InvalidParameterException => assert(e.getMessage == message)
    }
    try {
      val parseOutputStructFieldWithMetadataSource3Column: ParseOutput = TypeParser.standardize(structFieldWithMetadataSource3Column, path, schema)
      assert(false, "Should never get here- DecimalType")
    } catch {
      case e: InvalidParameterException => assert(e.getMessage == message)
    }
  }

  test("Timestamp fields without format") {
    val path: String = "path"
    val defaultFormat: String = Defaults.getGlobalFormat(TimestampType)
    val defaultValue: String = "1999-12-31 09:51:30"
    val defaultValueParsed: Long = 946633890000000L //this is the above timestamp transformed within the expression
    val structFieldNoDefault = StructField("field0", TimestampType)
    val structFieldWithDefaultMetadataColumn = StructField("field1", TimestampType, nullable = false, new MetadataBuilder().putString("default", defaultValue).build)
    val structFieldWithMetadataSource2Column = StructField("field2", TimestampType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field2_source").build)
    val structFieldWithMetadataSource3Column = StructField("field3", TimestampType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field3_source").build)
    val structFieldSource2Column = StructField("field2_source", DoubleType, nullable = false)
    val structFieldSource3Column = StructField("field3_source", DecimalType(10,2), nullable = false)
    val schema: StructType = StructType(Array(StructField(path, StructType(Array(structFieldNoDefault, structFieldWithDefaultMetadataColumn, structFieldWithMetadataSource2Column, structFieldWithMetadataSource3Column, structFieldSource2Column, structFieldSource3Column)))))

    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val parseOutputStructFieldNoDefault: ParseOutput = TypeParser.standardize(structFieldNoDefault, path, schema)
    assertStringOccurances(parseOutputStructFieldNoDefault, s"to_timestamp('$path.field0, $defaultFormat)", 3, 1)
    val parseOutputStructFieldWithMetadataNotSourceColumn: ParseOutput = TypeParser.standardize(structFieldWithDefaultMetadataColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataNotSourceColumn, s"to_timestamp('$path.field1, $defaultFormat)", 3, 1)
    assertStringOccurances(parseOutputStructFieldWithMetadataNotSourceColumn, s"ELSE $defaultValueParsed END", 1)
    val message = "Dates & times represented as numeric values need specified 'pattern' metadata"
    try {
      val parseOutputStructFieldWithMetadataSource2Column: ParseOutput = TypeParser.standardize(structFieldWithMetadataSource2Column, path, schema)
      assert(false, "Should never get here - DoubleType")
    } catch {
      case e: InvalidParameterException => assert(e.getMessage == message)
    }
    try {
      val parseOutputStructFieldWithMetadataSource3Column: ParseOutput = TypeParser.standardize(structFieldWithMetadataSource3Column, path, schema)
      assert(false, "Should never get here- DecimalType")
    } catch {
      case e: InvalidParameterException => assert(e.getMessage == message)
    }
  }

  test("Date fields with format") {
    val path: String = "path"
    val defaultValue1: String = "1999:31:12"
    val defaultValue2: String = "121999.31"
    val defaultValue3: String = "31121999"
    val defaultValueParsed: Integer = 10956 //this is the above date transformed within the expression
    val structFieldNoDefault = StructField("field0", DateType, nullable = false, new MetadataBuilder().putString("pattern", "dd/MM/yyyy").build)
    val structFieldWithDefaultMetadataColumn = StructField("field1", DateType, nullable = false, new MetadataBuilder().putString("default", defaultValue1).putString("pattern", "yyyy:dd:MM").build)
    val structFieldWithMetadataSource2Column = StructField("field2", DateType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field2_source").putString("pattern", "MMyyyy.dd").putString("default", defaultValue2).build)
    val structFieldWithMetadataSource3Column = StructField("field3", DateType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field3_source").putString("pattern", "ddMMyyyy").putString("default", defaultValue3).build)
    val structFieldSource2Column = StructField("field2_source", DoubleType, nullable = false)
    val structFieldSource3Column = StructField("field3_source", DecimalType(10,2), nullable = false)
    val schema: StructType = StructType(Array(StructField(path, StructType(Array(structFieldNoDefault, structFieldWithDefaultMetadataColumn, structFieldWithMetadataSource2Column, structFieldWithMetadataSource3Column, structFieldSource2Column, structFieldSource3Column)))))

    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val parseOutputStructFieldNoDefault: ParseOutput = TypeParser.standardize(structFieldNoDefault, path, schema)
    assertStringOccurances(parseOutputStructFieldNoDefault, s"to_date('$path.field0, Some(dd/MM/yyyy))", 3, 1)
    val parseOutputStructFieldWithMetadataNotSourceColumn: ParseOutput = TypeParser.standardize(structFieldWithDefaultMetadataColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataNotSourceColumn, s"to_date('$path.field1, Some(yyyy:dd:MM))", 3, 1)
    assertStringOccurances(parseOutputStructFieldWithMetadataNotSourceColumn, s"ELSE $defaultValueParsed END", 1)
    val parseOutputStructFieldWithMetadataSource2Column: ParseOutput = TypeParser.standardize(structFieldWithMetadataSource2Column, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataSource2Column, s"to_date(cast(cast('$path.field2_source as decimal(8,2)) as string), Some(MMyyyy.dd))", 3, 1)
    assertStringOccurances(parseOutputStructFieldWithMetadataSource2Column, s"ELSE $defaultValueParsed END", 1)
    val parseOutputStructFieldWithMetadataSource3Column: ParseOutput = TypeParser.standardize(structFieldWithMetadataSource3Column, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataSource3Column, s"to_date(cast('$path.field3_source as string), Some(ddMMyyyy))", 3, 1)
    assertStringOccurances(parseOutputStructFieldWithMetadataSource3Column, s"ELSE $defaultValueParsed END", 1)
  }

  test("Timestamp fields with format") {
    val path: String = "path"
    val defaultValue: String = "1999-12-31 09:51:30"
    val defaultValue1: String = "1999:31:12_09~51~30"
    val defaultValue2: String = "31121999.095130"
    val defaultValue3: String = "31121999095130"
    val defaultValueParsed: Long = 946633890000000L //this is the above timestamp transformed within the expression
    val structFieldNoDefault = StructField("field0", TimestampType, nullable = false, new MetadataBuilder().putString("pattern", "dd/MM/yyyy/hh/mm/ss").build)
    val structFieldWithDefaultMetadataColumn = StructField("field1", TimestampType, nullable = false, new MetadataBuilder().putString("default", defaultValue1).putString("pattern", "yyyy:dd:MM_hh~mm~ss").build)
    val structFieldWithMetadataSource2Column = StructField("field2", TimestampType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field2_source").putString("pattern", "ddMMyyyy.hhmmss").putString("default", defaultValue2).build)
    val structFieldWithMetadataSource3Column = StructField("field3", TimestampType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field3_source").putString("pattern", "ddMMyyyyhhmmss").putString("default", defaultValue3).build)
    val structFieldSource2Column = StructField("field2_source", DoubleType, nullable = false)
    val structFieldSource3Column = StructField("field3_source", DecimalType(10,2), nullable = false)
    val schema: StructType = StructType(Array(StructField(path, StructType(Array(structFieldNoDefault, structFieldWithDefaultMetadataColumn, structFieldWithMetadataSource2Column, structFieldWithMetadataSource3Column, structFieldSource2Column, structFieldSource3Column)))))

    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val parseOutputStructFieldNoDefault: ParseOutput = TypeParser.standardize(structFieldNoDefault, path, schema)
    assertStringOccurances(parseOutputStructFieldNoDefault, s"to_timestamp('$path.field0, dd/MM/yyyy/hh/mm/ss)", 3, 1)
    val parseOutputStructFieldWithMetadataNotSourceColumn: ParseOutput = TypeParser.standardize(structFieldWithDefaultMetadataColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataNotSourceColumn, s"to_timestamp('$path.field1, yyyy:dd:MM_hh~mm~ss)", 3, 1)
    assertStringOccurances(parseOutputStructFieldWithMetadataNotSourceColumn, s"ELSE $defaultValueParsed END", 1)
    val parseOutputStructFieldWithMetadataSource2Column: ParseOutput = TypeParser.standardize(structFieldWithMetadataSource2Column, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataSource2Column, s"to_timestamp(cast(cast('$path.field2_source as decimal(14,6)) as string), ddMMyyyy.hhmmss)", 3, 1)
    assertStringOccurances(parseOutputStructFieldWithMetadataSource2Column, s"ELSE $defaultValueParsed END", 1)
    val parseOutputStructFieldWithMetadataSource3Column: ParseOutput = TypeParser.standardize(structFieldWithMetadataSource3Column, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataSource3Column, s"to_timestamp(cast('$path.field3_source as string), ddMMyyyyhhmmss)", 3, 1)
    assertStringOccurances(parseOutputStructFieldWithMetadataSource3Column, s"ELSE $defaultValueParsed END", 1)
  }

  test("Date fields with epoch format") {
    val path: String = "path"
    val structFieldEpochColumn = StructField("field0", DateType, nullable = false, new MetadataBuilder().putString("pattern", "epoch").build)
    val structFieldMilliEpochColumn = StructField("field1", DateType, nullable = false, new MetadataBuilder().putString("pattern", "milliepoch").build)
    val structFieldWithMetadataSourceEpochColumn = StructField("field2", DateType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field2_source").putString("pattern", "epoch").build)
    val structFieldWithMetadataSourceMilliEpochColumn = StructField("field3", DateType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field3_source").putString("pattern", "milliepoch").build)
    val structFieldSource2Column = StructField("field2_source", IntegerType, nullable = false)
    val structFieldSource3Column = StructField("field3_source", DecimalType(16,0), nullable = false)
    val schema: StructType = StructType(Array(StructField(path, StructType(Array(structFieldEpochColumn, structFieldMilliEpochColumn, structFieldWithMetadataSourceEpochColumn, structFieldWithMetadataSourceMilliEpochColumn, structFieldSource2Column, structFieldSource3Column)))))

    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val parseOutputStructFieldEpoch: ParseOutput = TypeParser.standardize(structFieldEpochColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldEpoch, s"from_unixtime((cast('$path.field0 as bigint) / 1), yyyy-MM-dd, None)", 3, 1)
    val parseOutputStructFieldMilliEpoch: ParseOutput = TypeParser.standardize(structFieldMilliEpochColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldMilliEpoch, s"from_unixtime((cast('$path.field1 as bigint) / 1000), yyyy-MM-dd, None)", 3, 1)
    val parseOutputStructFieldWithMetadataSourceEpoch: ParseOutput = TypeParser.standardize(structFieldWithMetadataSourceEpochColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataSourceEpoch, s"from_unixtime((cast('$path.field2_source as bigint) / 1), yyyy-MM-dd, None)", 3, 1)
    val parseOutputStructFieldWithMetadataSourceMilliEpoch: ParseOutput = TypeParser.standardize(structFieldWithMetadataSourceMilliEpochColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataSourceMilliEpoch, s"from_unixtime((cast('$path.field3_source as bigint) / 1000), yyyy-MM-dd, None)", 3, 1)
  }

  test("Timestamp fields with epoch format") {
    val path: String = "path"
    val structFieldEpochColumn = StructField("field0", TimestampType, nullable = false, new MetadataBuilder().putString("pattern", "epoch").build)
    val structFieldMilliEpochColumn = StructField("field1", TimestampType, nullable = false, new MetadataBuilder().putString("pattern", "milliepoch").build)
    val structFieldWithMetadataSourceEpochColumn = StructField("field2", TimestampType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field2_source").putString("pattern", "epoch").build)
    val structFieldWithMetadataSourceMilliEpochColumn = StructField("field3", TimestampType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "field3_source").putString("pattern", "milliepoch").build)
    val structFieldSource2Column = StructField("field2_source", IntegerType, nullable = false)
    val structFieldSource3Column = StructField("field3_source", DecimalType(16,0), nullable = false)
    val schema: StructType = StructType(Array(StructField(path, StructType(Array(structFieldEpochColumn, structFieldMilliEpochColumn, structFieldWithMetadataSourceEpochColumn, structFieldWithMetadataSourceMilliEpochColumn, structFieldSource2Column, structFieldSource3Column)))))

    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val parseOutputStructFieldEpoch: ParseOutput = TypeParser.standardize(structFieldEpochColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldEpoch, s"from_unixtime((cast('$path.field0 as bigint) / 1), yyyy-MM-dd HH:mm:ss, None)", 3, 1)
    val parseOutputStructFieldMilliEpoch: ParseOutput = TypeParser.standardize(structFieldMilliEpochColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldMilliEpoch, s"from_unixtime((cast('$path.field1 as bigint) / 1000), yyyy-MM-dd HH:mm:ss, None)", 3, 1)
    val parseOutputStructFieldWithMetadataSourceEpoch: ParseOutput = TypeParser.standardize(structFieldWithMetadataSourceEpochColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataSourceEpoch, s"from_unixtime((cast('$path.field2_source as bigint) / 1), yyyy-MM-dd HH:mm:ss, None)", 3, 1)
    val parseOutputStructFieldWithMetadataSourceMilliEpoch: ParseOutput = TypeParser.standardize(structFieldWithMetadataSourceMilliEpochColumn, path, schema)
    assertStringOccurances(parseOutputStructFieldWithMetadataSourceMilliEpoch, s"from_unixtime((cast('$path.field3_source as bigint) / 1000), yyyy-MM-dd HH:mm:ss, None)", 3, 1)
  }

}
