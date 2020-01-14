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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.samples.DeepArraySamples
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.{ConcatenationConformanceRule, UppercaseConformanceRule}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ConcatenationRuleSuite extends FunSuite with SparkTestBase with TestRuleBehaviors {
  private val concatRule = ConcatenationConformanceRule(order = 1, outputColumn = "CombinedName",
    controlCheckpoint = false, Seq("name", "city", "address"))
  private val concatArrayRule = ConcatenationConformanceRule(order = 2, outputColumn = "rooms.CombinedLabel",
    controlCheckpoint = false, Seq("rooms.roomName", "rooms.label"))
  private val concatDeepArrayRule = ConcatenationConformanceRule(order = 3, outputColumn = "rooms.books.CombinedBooks",
    controlCheckpoint = false, Seq("rooms.books.author", "rooms.books.name"))

  private val concatRulesList = List(concatRule, concatArrayRule, concatDeepArrayRule)

  private val concatOrdersDS = Dataset(name = "Library Conformance", version = 1,
    hdfsPath = "src/test/testData/library", hdfsPublishPath =
    "testData/conformedLibrary",
    schemaName = "Library", schemaVersion = 1,
    conformance = concatRulesList)

  // scalastyle:off line.size.limit
  private val conformedConcatOrdersJSON: String =
    """{"id":1,"name":"National Library","city":"Washington","address":"10 Linkoln ave.","rooms":[{"roomName":"History","label":"H1","capacity":25000,"books":[{"author":"Christopher Browning","name":"Ordinary Men","CombinedBooks":"Christopher BrowningOrdinary Men"},{"author":"Vasily Grossman","name":"Life and Fate","CombinedBooks":"Vasily GrossmanLife and Fate"},{"author":"Homer","name":"Illiad","CombinedBooks":"HomerIlliad"}],"CombinedLabel":"HistoryH1"},{"roomName":"Politics","label":"P2","capacity":15000,"books":[{"author":"Michael Lewis","name":"The Fifths Risk","CombinedBooks":"Michael LewisThe Fifths Risk"},{"author":"Aristotle","name":"Politics","CombinedBooks":"AristotlePolitics"}],"CombinedLabel":"PoliticsP2"},{"roomName":"Sociology","label":"S3","capacity":8000,"books":[],"CombinedLabel":"SociologyS3"}],"errCol":[],"CombinedName":"National LibraryWashington10 Linkoln ave."}
      |{"id":2,"name":"Technical Library","city":"New York","address":"101/2 Park ave.","rooms":[{"roomName":"Math","label":"M41","capacity":10000,"books":[{"author":"C. C. Tisdell","name":"Introduction to Complex Numbers","CombinedBooks":"C. C. TisdellIntroduction to Complex Numbers"},{"author":"Michael Batty","name":"Essential Engineering Mathematics","CombinedBooks":"Michael BattyEssential Engineering Mathematics"},{"author":"Gareth J. Janecek","name":"Mathematics for Computer Scientist","CombinedBooks":"Gareth J. JanecekMathematics for Computer Scientist"}],"CombinedLabel":"MathM41"},{"roomName":"Physics","label":"P5","capacity":15000,"books":[{"author":"Ali R. Fazely","name":"Foundation of Physics for Scientists and Engineers","CombinedBooks":"Ali R. FazelyFoundation of Physics for Scientists and Engineers"},{"author":"Tarik Al-Shemmeri","name":"Engineering Thermodynamics","CombinedBooks":"Tarik Al-ShemmeriEngineering Thermodynamics"},{"author":"Satindar Bhagat","name":"Elementary Physics I","CombinedBooks":"Satindar BhagatElementary Physics I"}],"CombinedLabel":"PhysicsP5"},{"roomName":"IT","label":"I6","capacity":35000,"books":[{"author":"Garry Turkington","name":"Hadoop Beginner’s Guide","CombinedBooks":"Garry TurkingtonHadoop Beginner’s Guide"},{"author":"Thomas H. Davenport","name":"Big Data at Work","CombinedBooks":"Thomas H. DavenportBig Data at Work"},{"author":"Martin Kleppmann","name":"Designing Data-Intensive Applications","CombinedBooks":"Martin KleppmannDesigning Data-Intensive Applications"}],"CombinedLabel":"ITI6"}],"errCol":[],"CombinedName":"Technical LibraryNew York101/2 Park ave."}
      |{"id":3,"name":"Prazska Knihovna","city":"Prague","address":"Vikova 1223/4","rooms":[{"roomName":"Literature","label":"L7","capacity":55000,"books":[{"author":"James Joyce","name":"Ulysses","CombinedBooks":"James JoyceUlysses"},{"author":"Herman Melville","name":"Moby Dick","CombinedBooks":"Herman MelvilleMoby Dick"},{"author":"William Shakespeare","name":"Hamlet","CombinedBooks":"William ShakespeareHamlet"}],"CombinedLabel":"LiteratureL7"},{"roomName":"Poetry","label":"P8","capacity":35000,"books":[{"author":"Simon Armitage","name":"The Unaccompanied","CombinedBooks":"Simon ArmitageThe Unaccompanied"},{"author":"D. Nurkse","name":"Love in the Last Days","CombinedBooks":"D. NurkseLove in the Last Days"},{"author":"Federico García Lorca","name":"Poet in Spain","CombinedBooks":"Federico García LorcaPoet in Spain"}],"CombinedLabel":"PoetryP8"}],"errCol":[],"CombinedName":"Prazska KnihovnaPragueVikova 1223/4"}"""
      .stripMargin.replace("\r\n", "\n")

  private val inputDf: DataFrame = spark.createDataFrame(DeepArraySamples.libraryData)

  test("Concatenation conformance rule test") {
    conformanceRuleShouldMatchExpected(inputDf, concatOrdersDS, conformedConcatOrdersJSON)
  }

}
