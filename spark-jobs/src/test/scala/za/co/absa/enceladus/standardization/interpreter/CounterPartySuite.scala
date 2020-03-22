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

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

case class Root(ConformedParty: Party, errCol: Seq[ErrorMessage] = Seq.empty)
case class Party(key: Integer, clientKeys1: Seq[String], clientKeys2: Seq[String])

class CounterPartySuite extends FunSuite with SparkTestBase with LoggerTestBase {

  test("Mimic running standardization twice on counter party") {
    import spark.implicits._

    val desiredSchema = StructType(Seq(StructField("ConformedParty", StructType(
      Seq(
        StructField("key", IntegerType, nullable = true),
        StructField("clientKeys1", ArrayType(StringType, containsNull = true), nullable = true)
        ,
        StructField("clientKeys2", ArrayType(StringType, containsNull = true), nullable = true)

      )), nullable = true)))

    implicit val udfLib: UDFLibrary = new UDFLibrary

    val input = spark.createDataFrame(Seq(
      Root(Party(key = 0, clientKeys1 = Seq("a", "b", "c"), clientKeys2 = Seq("d", "e", "f"))),
      Root(Party(1, Seq("d"), Seq("e"))),
      Root(Party(2, Seq("f"), Seq())),
      Root(Party(3, Seq(), Seq())),
      Root(Party(4, null, Seq())),
      Root(Party(5, Seq(), null)),
      Root(Party(6, null, null))))

    val std = StandardizationInterpreter.standardize(input, desiredSchema, "", failFast = false).cache()

    logDataFrameContent(std)

    assertResult(input.as[Root].collect.toList)(std.as[Root].collect().sortBy(_.ConformedParty.key).toList)
  }
}
