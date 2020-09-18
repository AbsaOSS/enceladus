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

package za.co.absa.enceladus.conformance.samples

import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.model.{MappingTable, Dataset => ConfDataset}
import za.co.absa.enceladus.utils.error._

case class Outer(order: Int, a: Seq[Inner], myFlag: Boolean)
case class OuterErr(order: Int, a: Seq[Inner], myFlag: Boolean, errCol: Seq[ErrorMessage])
case class Inner(c: Seq[Inner2])
case class Inner2(d: Int, toDrop: String = "Drop me :)")

case class ToJoin(ind: Int, value: String, otherFlag: String = "true")
case class ConformedOuter(order: Int, a: Seq[ConformedInner], errCol: Seq[ErrorMessage])
case class ConformedInner(c: Seq[ConformedInner2])
case class ConformedInner3(myInnerThingy: String)
case class ConformedInner2(d: Int, conformedD: String, conformedConcat: String, conformedLit: String, conformedStruct: ConformedInner3, conformedSparkConf: String, conformedUpper: String)

object MappingsSamples {

  val mapping = Seq(
    ToJoin(1, "one"), ToJoin(2, "two"), ToJoin(3, "three"), ToJoin(4, "four"), ToJoin(5, "five"), ToJoin(6, "six")
  )

  val mappingTable = new MappingTable(name = "mapping", version = 0, hdfsPath = "mapping", schemaName = "mapping", schemaVersion = 0, defaultMappingValue = List())
}

object NullArraySamples {

  val testData = Seq(
    Outer(0, null, true),
    Outer(1, Seq(
      Inner(null)
    ), true)
  )

  val mappingOnlyConformanceDef = ConfDataset(name="test", version = 0, hdfsPath = "test", hdfsPublishPath = "", schemaName = "test", schemaVersion = 0,
    conformance = List(
      new MappingConformanceRule(order = 1, controlCheckpoint = false, mappingTable = "mapping", mappingTableVersion = 0,
        attributeMappings = Map("ind" -> "a.c.d", "otherFlag" -> "myFlag"), targetAttribute = "value", outputColumn = "a.c.conformedD" )
    )
  )

  // This is the total number of errors in the error column the conformed dataset should have
  val totalNumberOfErrors = 0

  val conformedData = Seq(
    OuterErr(0, null, true, Seq()),
    OuterErr(1, Seq(Inner(null)), true, Seq())
  )
}

object EmtpyArraySamples {

  val testData = Seq(
    Outer(0, Seq(), true),
    Outer(1, Seq(
      Inner(Seq())
    ), true)
  )

  val mappingOnlyConformanceDef = ConfDataset(name="test", version = 0, hdfsPath = "test", hdfsPublishPath = "", schemaName = "test", schemaVersion = 0,
    conformance = List(
      new MappingConformanceRule(order = 1, controlCheckpoint = false, mappingTable = "mapping", mappingTableVersion = 0,
        attributeMappings = Map("ind" -> "a.c.d", "otherFlag" -> "myFlag"), targetAttribute = "value", outputColumn = "a.c.conformedD" )
    )
  )

  // This is the total number of errors in the error column the conformed dataset should have
  val totalNumberOfErrors = 0

  val conformedData = Seq(
    OuterErr(0, Seq(), true, Seq()),
    OuterErr(1, Seq(
      Inner(Seq())
    ), true, Seq())
  )
}

object ArraySamples {

  val testData = Seq(
    Outer(0, null, true),
    Outer(1,
      Seq(
        Inner(
          Seq(Inner2(0), Inner2(1), Inner2(2))),
        Inner(Seq()),
        Inner(null)), true),
    Outer(2, Seq(), false),
    Outer(3, null, true))

  val testDataMult2 = Seq(
    Outer(0,
      Seq(
        Inner(
          Seq(Inner2(0), Inner2(2), Inner2(4))),
        Inner(Seq()),
        Inner(null)), true),
    Outer(1, Seq(), false),
    Outer(2, null, true))

    val conformanceDef = ConfDataset(name="test", version = 0, hdfsPath = "test", hdfsPublishPath = "", schemaName = "test", schemaVersion = 0,
      conformance = List(
        new MappingConformanceRule(order = 1, controlCheckpoint = false, mappingTable = "mapping", mappingTableVersion = 0,
          attributeMappings = Map("ind" -> "a.c.d", "otherFlag" -> "myFlag"), targetAttribute = "value", outputColumn = "a.c.conformedD" ),
        new ConcatenationConformanceRule(order = 2, controlCheckpoint = false, outputColumn = "a.c.conformedConcat", inputColumns = List("a.c.conformedD", "a.c.toDrop")),
        new LiteralConformanceRule(order = 4, controlCheckpoint = false, outputColumn = "a.c.toDropNonOriginal", value = "DropThis"),
        new DropConformanceRule(order = 3, controlCheckpoint = false, outputColumn = "a.c.toDropNonOriginal"),
        new LiteralConformanceRule(order = 4, controlCheckpoint = false, outputColumn = "a.c.conformedLit", value = "Hello world"),
        new SingleColumnConformanceRule(order = 5, controlCheckpoint = false, outputColumn = "a.c.conformedStruct", inputColumn = "a.c.conformedLit", inputColumnAlias = "myInnerThingy"),
        new SparkSessionConfConformanceRule(order = 6, controlCheckpoint = false, outputColumn = "a.c.conformedSparkConf", sparkConfKey = "za.co.absa.myVal"),
        new UppercaseConformanceRule(order = 7, controlCheckpoint = false, outputColumn = "a.c.conformedUpper", inputColumn = "a.c.conformedLit")
      )
    )

    // This is the total number of errors in the error column the conformed dataset should have
    val totalNumberOfErrors = 1

    val conformedData = Seq(
      ConformedOuter(0, null, Seq()),
      ConformedOuter(1,
        Seq(
          ConformedInner(
            Seq(
                ConformedInner2(0, null, null, "Hello world", new ConformedInner3("Hello world"), "myConf", "HELLO WORLD"),
                ConformedInner2(1, "one", "oneDrop me :)", "Hello world",new ConformedInner3("Hello world"), "myConf", "HELLO WORLD"),
                ConformedInner2(2, "two", "twoDrop me :)", "Hello world", new ConformedInner3("Hello world"), "myConf", "HELLO WORLD")
            )),
          ConformedInner(Seq()),
          ConformedInner(null)), Seq(ErrorMessage.confMappingErr("a.c.conformedD", List("0", "true"), List(Mapping("ind", "a.c.d"), Mapping("otherFlag", "myFlag"))))),
      ConformedOuter(2, Seq(), Seq()),
      ConformedOuter(3, null, Seq()))
}
