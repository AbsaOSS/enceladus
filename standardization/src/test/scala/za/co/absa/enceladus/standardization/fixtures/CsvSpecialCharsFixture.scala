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

package za.co.absa.enceladus.standardization.fixtures

import java.io.{DataOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, Suite}
import za.co.absa.enceladus.model.Dataset

trait CsvSpecialCharsFixture extends BeforeAndAfterAll {

  this: Suite =>

  val csvContent: String =
    """1¡2¡3¡4¡5
      |Text1¡Text2¡Text3¡10¡11
      |Text5¡Text6¡Text7¡-99999¡99999
      |Text10"Add¡Text11¡Text12¡100¡200
      |"Text15¡Text16¡Text17¡1000¡2000"""
      .stripMargin

  val schema: StructType = StructType(Seq(
    StructField("A1", StringType, nullable = true),
    StructField("A2", StringType, nullable = true),
    StructField("A3", StringType, nullable = true),
    StructField("A4", IntegerType, nullable = true),
    StructField("A5", IntegerType, nullable = true)
  ))

  val inputFileName: String = getTempFile

  val dataSet = Dataset("SpecialChars", 1, None, inputFileName, "", "SpecialChars", 1, conformance = Nil)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createExampleFile()
  }

  override def afterAll(): Unit = {
    deleteExampleFile()
    super.afterAll()
  }

  private def createExampleFile(): Unit = {
    val ostream = new DataOutputStream(new FileOutputStream(inputFileName))
    ostream.write(csvContent.getBytes(StandardCharsets.ISO_8859_1))
    ostream.close()
  }

  private def deleteExampleFile(): Unit = {
    val file = new File(inputFileName)
    file.delete()
  }

  private def getTempFile: String = {
    val tempFile = File.createTempFile("csv-special-chars-", ".csv")
    tempFile.getAbsolutePath
  }

}
