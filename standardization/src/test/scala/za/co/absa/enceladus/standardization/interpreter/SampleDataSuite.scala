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

package za.co.absa.enceladus.standardization.interpreter

import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.standardization.samples.{StdEmployee, TestSamples}
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

import scala.io.Source

class SampleDataSuite extends FunSuite with SparkTestBase {

  test("Simple Example Test") {
    import spark.implicits._
    val data = spark.createDataFrame(TestSamples.data1)
    
    data.printSchema()    
    
    implicit val udfLib: UDFLibrary = UDFLibrary()

    val schema = DataType.fromJson(Source.fromFile("src/test/resources/data1Schema.json").getLines().mkString("\n")).asInstanceOf[StructType]

    val std = StandardizationInterpreter.standardize(data, schema, "whatev").as[StdEmployee].collect.sortBy(_.name).toList
    val exp = TestSamples.resData.sortBy(_.name)
    
//    std.show(false)
//    std.printSchema()
    
    assertResult(exp)(std)

  }

}
