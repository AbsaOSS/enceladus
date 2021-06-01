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

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.mockito.scalatest.MockitoSugar
import org.scalatest.Outcome
import za.co.absa.enceladus.common.RecordIdGeneration.IdType
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.standardization.fixtures.TempFileFixture
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.TypeParserException
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.udf.UDFLibrary
import StandardizationParquetSuite._
import za.co.absa.enceladus.utils.testUtils.DataFrameTestUtils._

class StandardizationParquetSuite extends FixtureAnyFunSuite with SparkTestBase with TempFileFixture with MockitoSugar with DatasetComparer  {
  type FixtureParam = String


  import spark.implicits._
  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

  private val standardizationReader = new StandardizationPropertiesProvider()
  private implicit val dao: MenasDAO = mock[MenasDAO]
  private implicit val udfLibrary:UDFLibrary = new UDFLibrary()

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
                               args: Array[String]): (StandardizationConfig, DataFrame) = {
    val cmd: StandardizationConfig = StandardizationConfig.getFromArguments(args)
    val csvReader = standardizationReader.getFormatSpecificReader(cmd, dataSet)
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

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    val actualDf = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val expectedData = Seq(
      Row(1L, Array("A", "B"), Row(false), Array()),
      Row(2L, Array("C"), Row(true), Array())
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Missing nullable fields are considered null") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

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
    val actualDf = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val expectedData = Seq(
      Row(1, null, null, null, null, null, Array()),
      Row(2, null, null, null, null, null, Array())
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Missing non-nullable fields are filled with default values and error appears in error column") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

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
    val actualDf = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val EpochTimestamp = Timestamp.from(Instant.EPOCH)
    val expectedErrors = Seq(
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "string_field", Seq("null"), Seq()),
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "timestamp_field", Seq("null"), Seq()),
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "long_field", Seq("null"), Seq()),
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "double_field", Seq("null"), Seq()),
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "decimal_field", Seq("null"), Seq())
    )
    val expectedData = Seq(
      Row(1, "", EpochTimestamp, 0L, 0, Decimal(3.14), expectedErrors),
      Row(2, "", EpochTimestamp, 0L, 0, Decimal(3.14), expectedErrors)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Cannot convert int to array, and array to long") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

  val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", ArrayType(StringType), nullable = true),
      StructField("letters", LongType, nullable = true),
      StructField("lettersB", LongType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.SourceColumn, "letters").build())
    )
    val schema = StructType(seq)
    val actualDf = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val expectedErrors = Seq(
      Row("stdTypeError", "E00006", "Standardization Error - Type 'integer' cannot be cast to 'array'", "id", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'array' cannot be cast to 'long'", "letters", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'array' cannot be cast to 'long'", "letters", Seq(), Seq())
    )
    val expectedData = Seq(
      Row(null, null, 0L, expectedErrors),
      Row(null, null, 0L, expectedErrors)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Cannot convert int to struct, and struct to long") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

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
    val actualDf = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val expectedErrors = Seq(
      Row("stdTypeError", "E00006", "Standardization Error - Type 'integer' cannot be cast to 'struct'", "id", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'struct' cannot be cast to 'long'", "struct", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'struct' cannot be cast to 'long'", "struct", Seq(), Seq())
    )
    val expectedData = Seq(
      Row(null, null, -1L, expectedErrors),
      Row(null, null, -1L, expectedErrors)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Cannot convert array to struct, and struct to array") { tmpFileName =>
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
    val actualDf = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat)

    val expectedErrors = Seq(
      Row("stdTypeError", "E00006", "Standardization Error - Type 'array' cannot be cast to 'struct'", "letters", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'struct' cannot be cast to 'array'", "struct", Seq(), Seq())
    )
    val expectedData = Seq(
      Row(1L, null, null, expectedErrors),
      Row(2L, null, null, expectedErrors)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Cannot convert int to array, and array to long, fail fast") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

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

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    // stableHashId will always yield the same ids
    val actualDf = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat, recordIdGenerationStrategy = IdType.StableHashId)

    val expectedData = Seq(
      Row(1L, Array("A", "B"), Row(false), Array(), 1950798873),
      Row(2L, Array("C"), Row(true), Array(), -988631025)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("True uuids are used") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    val actualDf = StandardizationInterpreter.standardize(sourceDF, schema, cmd.rawFormat, recordIdGenerationStrategy = IdType.TrueUuids)

    val expectedData = Seq(
      Row(1L, Array("A", "B"), Row(false), Array()),
      Row(2L, Array("C"), Row(true), Array())
    )
    // checking just the data without enceladus_record_id, not the schema here
    val expectedDF = expectedData.toDfWithSchema(actualDf.drop("enceladus_record_id").schema)

    // same except for the record id
    assertSmallDatasetEquality(actualDf.drop("enceladus_record_id"), expectedDF, ignoreNullable = true)

    val destIds = actualDf.select('enceladus_record_id).collect().map(_.getAs[String](0)).toSet
    assert(destIds.size == 2)
    destIds.foreach(UUID.fromString) // check uuid validity

  }

  test("Existing enceladus_record_id is kept") { tmpFileName =>
    val args = (s"--dataset-name $datasetName --dataset-version $datasetVersion --report-date 2019-07-23" +
      " --report-version 1 --menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format parquet").split(" ")

    val (cmd, sourceDF) = getTestDataFrame(tmpFileName, args)
    import org.apache.spark.sql.functions.{concat, lit}
    val sourceDfWithExistingIds = sourceDF.withColumn("enceladus_record_id", concat(lit("id"), 'id))

    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false),
      StructField("enceladus_record_id", StringType, nullable = false)
    )
    val schema = StructType(seq)
    val actualDf = StandardizationInterpreter.standardize(sourceDfWithExistingIds, schema, cmd.rawFormat, recordIdGenerationStrategy = IdType.TrueUuids)

    // The TrueUuids strategy does not override the existing values
    val expectedData = Seq(
      Row(1L, Array("A", "B"), Row(false), "id1", Array()),
      Row(2L, Array("C"), Row(true), "id2", Array())
    )

    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

}

object StandardizationParquetSuite {
  private case class FooClass(bar: Boolean)

}
