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

package za.co.absa.enceladus.standardization

import java.util.UUID

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Outcome, fixture}
import za.co.absa.enceladus.common.RecordIdGeneration.UuidType
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.fixtures.TempFileFixture
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.TypeParserException
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.udf.UDFLibrary

class StandardizationParquetSuite extends fixture.FunSuite with SparkTestBase with TempFileFixture with MockitoSugar  {
  type FixtureParam = String


  import spark.implicits._
  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

  private implicit val dao: MenasDAO = mock[MenasDAO]
  private implicit val udfLibrary:UDFLibrary = UDFLibrary()

  private val tmpFilePrefix = "parquet-data-"
  private val datasetName = "ParquetTest"
  private val datasetVersion = 1


  private val data = Seq (
    (1, Array("A", "B"), FooClass(false)),
    (2, Array("C"), FooClass(true))
  )
  private val sourceDataDF = data.toDF("id", "letters", "struct")
  private val dataSet = Dataset(name = datasetName,
                                version = datasetVersion,
                                description = None,
                                hdfsPath = "",
                                hdfsPublishPath = "",
                                schemaName = datasetName,
                                schemaVersion = 1,
                                conformance = Nil)

  /** Creates a dataframe from an input file name path and command line arguments to Standardization */
  private def getTestDataFrame(tmpFileName: String,
                               args: Array[String]): (StdCmdConfig, DataFrame) = {
    val cmd: StdCmdConfig = StdCmdConfig.getCmdLineArguments(args)
    val csvReader = StandardizationJob.getFormatSpecificReader(cmd, dataSet)
    (cmd, csvReader.load(tmpFileName).orderBy("id"))
  }

  def withFixture(test: OneArgTest): Outcome = {
    val tempDir = createTempParquetFile(tmpFilePrefix, sourceDataDF)
    test(tempDir.getAbsolutePath)
  }

  test("All columns standardized") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """+---+-------+-------+------+
         ||id |letters|struct |errCol|
         |+---+-------+-------+------+
         ||1  |[A, B] |[false]|[]    |
         ||2  |[C]    |[true] |[]    |
         |+---+-------+-------+------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    val destDF = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }


  test("Missing nullable fields are considered null") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """+---+------------+---------------+----------+------------+-------------+------+
         ||id |string_field|timestamp_field|long_field|double_field|decimal_field|errCol|
         |+---+------------+---------------+----------+------------+-------------+------+
         ||1  |null        |null           |null      |null        |null         |[]    |
         ||2  |null        |null           |null      |null        |null         |[]    |
         |+---+------------+---------------+----------+------------+-------------+------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("string_field", StringType, nullable = true),
      StructField("timestamp_field", TimestampType, nullable = true),
      StructField("long_field", LongType, nullable = true),
      StructField("double_field", IntegerType, nullable = true),
      StructField("decimal_field", DecimalType(20,4), nullable = true)
    )
    val schema = StructType(seq)
    val destDF = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Missing non-nullable fields are filled with default values and error appears in error column") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """+---+------------+-------------------+----------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id |string_field|timestamp_field    |long_field|double_field|decimal_field|errCol                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
         |+---+------------+-------------------+----------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||1  |            |1970-01-01 00:00:00|0         |0           |3.14         |[[stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, string_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, timestamp_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, long_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, double_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, decimal_field, [null], []]]|
         ||2  |            |1970-01-01 00:00:00|0         |0           |3.14         |[[stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, string_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, timestamp_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, long_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, double_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, decimal_field, [null], []]]|
         |+---+------------+-------------------+----------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("string_field", StringType, nullable = false),
      StructField("timestamp_field", TimestampType, nullable = false),
      StructField("long_field", LongType, nullable = false),
      StructField("double_field", IntegerType, nullable = false),
      StructField("decimal_field",
        DecimalType(20,2),
        nullable = false,
        new MetadataBuilder().putString(MetadataKeys.DefaultValue, "3.14").build())
    )
    val schema = StructType(seq)
    val destDF = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Cannot convert int to array, and array to long") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id  |letters|lettersB|errCol                                                                                                                                                                                                                                                                                                                |
         |+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||null|null   |0       |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'array', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []]]|
         ||null|null   |0       |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'array', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []]]|
         |+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", ArrayType(StringType), nullable = true),
      StructField("letters", LongType, nullable = true),
      StructField("lettersB", LongType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.SourceColumn, "letters").build())
    )
    val schema = StructType(seq)
    val destDF = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Cannot convert int to struct, and struct to long") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """|+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id  |struct|structB|errCol                                                                                                                                                                                                                                                                                                                 |
         |+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||null|null  |-1     |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'struct', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []]]|
         ||null|null  |-1     |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'struct', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []]]|
         |+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", LongType, nullable = true),
      StructField("structB", LongType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, "struct")
        .putString(MetadataKeys.DefaultValue, "-1")
        .build())
    )
    val schema = StructType(seq)
    val destDF = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Cannot convert array to struct, and struct to array") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """|+---+-------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id |letters|struct|errCol                                                                                                                                                                                                             |
         |+---+-------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||1  |null   |null  |[[stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'struct', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'array', struct, [], []]]|
         ||2  |null   |null  |[[stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'struct', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'array', struct, [], []]]|
         |+---+-------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", LongType, nullable = true),
      StructField("letters", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", ArrayType(StringType), nullable = true)
    )
    val schema = StructType(seq)
    val destDF = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Cannot convert int to array, and array to long, fail fast") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||id  |letters|lettersB|errCol                                                                                                                                                                                                                                                                                                                |
        |+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||null|null   |0       |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'array', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []]]|
        ||null|null   |0       |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'array', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []]]|
        |+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", ArrayType(StringType), nullable = true),
      StructField("letters", LongType, nullable = true),
      StructField("lettersB", LongType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.SourceColumn, "letters").build())
    )
    val schema = StructType(seq)

    val exception = intercept[TypeParserException] {
      StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat, failOnInputNotPerSchema = true)
    }
    assert(exception.getMessage == "Cannot standardize field 'id' from type integer into array")
  }

  test("Cannot convert int to struct, and struct to long, fail fast") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """|+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id  |struct|structB|errCol                                                                                                                                                                                                                                                                                                                 |
         |+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||null|null  |-1     |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'struct', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []]]|
         ||null|null  |-1     |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'struct', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []]]|
         |+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", LongType, nullable = true),
      StructField("structB", LongType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, "struct")
        .putString(MetadataKeys.DefaultValue, "-1")
        .build())
    )
    val schema = StructType(seq)

    val exception = intercept[TypeParserException] {
      StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat, failOnInputNotPerSchema = true)
    }
    assert(exception.getMessage == "Cannot standardize field 'id' from type integer into struct")
  }

  test("Cannot convert array to struct, and struct to array, fail fast") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", LongType, nullable = true),
      StructField("letters", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", ArrayType(StringType), nullable = true)
    )
    val schema = StructType(seq)

    val exception = intercept[TypeParserException] {
      StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat, failOnInputNotPerSchema = true)
    }
    assert(exception.getMessage == "Cannot standardize field 'letters' from type array into struct")
  }

  test("PseudoUuids are used") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """+---+-------+-------+------+------------------------------------+
        ||id |letters|struct |errCol|enceladus_record_id                 |
        |+---+-------+-------+------+------------------------------------+
        ||1  |[A, B] |[false]|[]    |4fb44dab-0e1b-3f7e-8d93-a53862cd85c5|
        ||2  |[C]    |[true] |[]    |0f25bab5-3150-32eb-a2fd-09ad3d462f90|
        |+---+-------+-------+------+------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    // PseudoUuids will always yield the same ids
    val destDF = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat, recordIdGenerationStrategy = UuidType.PseudoUuids)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("True uuids are used") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val expected =
      """+---+-------+-------+------+
        ||id |letters|struct |errCol|
        |+---+-------+-------+------+
        ||1  |[A, B] |[false]|[]    |
        ||2  |[C]    |[true] |[]    |
        |+---+-------+-------+------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    // PseudoUuids will always yield the same ids
    val destDF = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat, recordIdGenerationStrategy = UuidType.TrueUuids)

    // same except for the record id
    val actual = destDF.drop("enceladus_record_id").dataAsString(truncate = false)
    assert(actual == expected)

    val destIds = destDF.select('enceladus_record_id ).collect().map(_.getAs[String](0)).toSet
    assert(destIds.size == 2)
    destIds.foreach(UUID.fromString) // check uuid validity

  }
}

private case class FooClass(bar: Boolean)
