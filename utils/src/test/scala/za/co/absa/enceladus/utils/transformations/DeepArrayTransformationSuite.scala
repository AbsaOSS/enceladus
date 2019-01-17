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

package za.co.absa.enceladus.utils.transformations

import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import org.apache.spark.sql.functions._

// Examples for constructing dataframes containing arrays of various levels of nestness

// Structs of Structs example
case class Address(city: String, street: String)

case class Employee(name: String, address: Address)

case class TestObj(id: Int, employee: Employee)

// Arrays of primitives example
case class FunWords(id: Int, words: Seq[String])

// Arrays of arrays of primitives example
case class GeoData(id: Int, matrix: Seq[Seq[String]])

// Arrays of structs example
case class Person(firstName: String, lastName: String)

case class Team(id: Int, person: Seq[Person])

case class Dept(name: String, team: Team)

// Arrays of Arrays of struct
case class Tournament(id: Int, person: Seq[Seq[Person]])

// Arrays of structs in arrays of structs
case class Condition(conif: String, conthen: String, amount: Double)

case class Leg(legid: Int, conditions: Seq[Condition])

case class Trade(id: Int, legs: Seq[Leg])

class DeepArrayTransformationSuite extends FunSuite with SparkTestBase {

  test("Test two level of struct nestness") {
    import spark.implicits._

    // Struct of struct
    val df = spark.sparkContext.parallelize(
      Seq(
        TestObj(1,Employee("Martin", Address("Olomuc", "Vodickova"))),
        TestObj(1,Employee("Petr", Address("Ostrava", "Vlavska"))),
        TestObj(1,Employee("Vojta", Address("Plzen", "Kralova")))
      )).toDF

    //df.printSchema()
    //df.toJSON.take(10).foreach(println)

    val dfOut = ArrayTransformations.nestedWithColumnMap(df, "employee.address.city", "conformedCity", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema = """root
                           | |-- id: integer (nullable = false)
                           | |-- employee: struct (nullable = false)
                           | |    |-- name: string (nullable = true)
                           | |    |-- address: struct (nullable = false)
                           | |    |    |-- city: string (nullable = true)
                           | |    |    |-- street: string (nullable = true)
                           | |    |    |-- conformedCity: string (nullable = true)
                           |""".stripMargin
    val expectedResults = "{\"id\":1,\"employee\":{\"name\":\"Martin\",\"address\":{\"city\":\"Olomuc\",\"street\":\"Vodickova\"," +
      "\"conformedCity\":\"OLOMUC\"}}}\n{\"id\":1,\"employee\":{\"name\":\"Petr\",\"address\":{\"city\":\"Ostrava\"," +
      "\"street\":\"Vlavska\",\"conformedCity\":\"OSTRAVA\"}}}\n{\"id\":1,\"employee\":{\"name\":\"Vojta\",\"address\"" +
      ":{\"city\":\"Plzen\",\"street\":\"Kralova\",\"conformedCity\":\"PLZEN\"}}}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test ("Test arrays of primitives") {
    import spark.implicits._
    // Array of primitives
    val df = spark.sparkContext.parallelize(
      Seq(
        FunWords(1,Seq("Gizmo", "Blurp", "Buzinga")),
        FunWords(1,Seq("Quirk", "Zap", "Mmrnmhrm"))
      )).toDF

    //df.printSchema()
    //df.toJSON.take(10).foreach(println)

    val dfOut = ArrayTransformations.nestedWithColumnMap(df, "words", "conformedWords", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema = """root
                           | |-- id: integer (nullable = false)
                           | |-- words: array (nullable = true)
                           | |    |-- element: string (containsNull = true)
                           | |-- conformedWords: array (nullable = true)
                           | |    |-- element: string (containsNull = true)
                           |""".stripMargin
    val expectedResults = "{\"id\":1,\"words\":[\"Gizmo\",\"Blurp\",\"Buzinga\"],\"conformedWords\":[\"GIZMO\",\"BLURP\"," +
      "\"BUZINGA\"]}\n{\"id\":1,\"words\":[\"Quirk\",\"Zap\",\"Mmrnmhrm\"],\"conformedWords\":[\"QUIRK\",\"ZAP\",\"MMRNMHRM\"]}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }


  test ("Test arrays of arrays of primitives") {
    import spark.implicits._
    // Array of arrays of primitives
    val df = spark.sparkContext.parallelize(
      Seq(
        GeoData(1,Seq(Seq("Tree", "Table"), Seq("Map", "Duck"))),
        GeoData(2,Seq(Seq("Apple", "Machine"), Seq("List", "Duck"))),
        GeoData(3,Seq(Seq("Computer", "Snake"), Seq("Sun", "Star")))
      )).toDF

    //df.printSchema()
    //df.toJSON.take(10).foreach(println)

    val dfOut = ArrayTransformations.nestedWithColumnMap(df, "matrix", "conformedMatrix", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema = """root
                           | |-- id: integer (nullable = false)
                           | |-- matrix: array (nullable = true)
                           | |    |-- element: array (containsNull = true)
                           | |    |    |-- element: string (containsNull = true)
                           | |-- conformedMatrix: array (nullable = true)
                           | |    |-- element: array (containsNull = true)
                           | |    |    |-- element: string (containsNull = true)
                           |""".stripMargin
    val expectedResults = "{\"id\":1,\"matrix\":[[\"Tree\",\"Table\"],[\"Map\",\"Duck\"]],\"conformedMatrix\":[[\"TREE\"," +
      "\"TABLE\"],[\"MAP\",\"DUCK\"]]}\n{\"id\":2,\"matrix\":[[\"Apple\",\"Machine\"],[\"List\",\"Duck\"]]," +
      "\"conformedMatrix\":[[\"APPLE\",\"MACHINE\"],[\"LIST\",\"DUCK\"]]}\n{\"id\":3,\"matrix\":[[\"Computer\"," +
      "\"Snake\"],[\"Sun\",\"Star\"]],\"conformedMatrix\":[[\"COMPUTER\",\"SNAKE\"],[\"SUN\",\"STAR\"]]}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test ("Test arrays of structs") {
    import spark.implicits._

    // Array of struct
    val df = spark.sparkContext.parallelize(
      Seq(
        Team(1,Seq(Person("John", "Smith"), Person("Jack", "Brown"))),
        Team(1,Seq(Person("Merry", "Cook"), Person("Jane", "Clark")))
      )).toDF

    //df.printSchema()
    //df.toJSON.take(10).foreach(println)

    val dfOut = ArrayTransformations.nestedWithColumnMap(df, "person.firstName", "conformedName", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema = """root
                           | |-- id: integer (nullable = false)
                           | |-- person: array (nullable = true)
                           | |    |-- element: struct (containsNull = false)
                           | |    |    |-- firstName: string (nullable = true)
                           | |    |    |-- lastName: string (nullable = true)
                           | |    |    |-- conformedName: string (nullable = true)
                           |""".stripMargin
    val expectedResults = "{\"id\":1,\"person\":[{\"firstName\":\"John\",\"lastName\":\"Smith\",\"conformedName\":\"JOHN\"}," +
      "{\"firstName\":\"Jack\",\"lastName\":\"Brown\",\"conformedName\":\"JACK\"}]}\n{\"id\":1,\"person\":[{\"firstName\":" +
      "\"Merry\",\"lastName\":\"Cook\",\"conformedName\":\"MERRY\"},{\"firstName\":\"Jane\",\"lastName\":\"Clark\"," +
      "\"conformedName\":\"JANE\"}]}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test arrays of arrays of struct") {
    import spark.implicits._

    // Array of struct
    val df = spark.sparkContext.parallelize(
      Seq(
        Tournament(1,Seq(Seq(Person("Mona Lisa", "Harddrive")), Seq(Person("Lenny", "Linux"), Person("Dot", "Not")) )),
        Tournament(1,Seq(Seq(Person("Eddie", "Larrison")), Seq(Person("Scarlett", "Johanson"), Person("William", "Windows")) ))
      )).toDF

    //df.printSchema()
    //df.toJSON.take(10).foreach(println)

    val dfOut = ArrayTransformations.nestedWithColumnMap(df, "person.lastName", "conformedName", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema = """root
                           | |-- id: integer (nullable = false)
                           | |-- person: array (nullable = true)
                           | |    |-- element: array (containsNull = true)
                           | |    |    |-- element: struct (containsNull = false)
                           | |    |    |    |-- firstName: string (nullable = true)
                           | |    |    |    |-- lastName: string (nullable = true)
                           | |    |    |    |-- conformedName: string (nullable = true)
                           |""".stripMargin
    val expectedResults = "{\"id\":1,\"person\":[[{\"firstName\":\"Mona Lisa\",\"lastName\":\"Harddrive\",\"conformedName\":" +
      "\"HARDDRIVE\"}],[{\"firstName\":\"Lenny\",\"lastName\":\"Linux\",\"conformedName\":\"LINUX\"},{\"firstName\":\"Dot\"," +
      "\"lastName\":\"Not\",\"conformedName\":\"NOT\"}]]}\n{\"id\":1,\"person\":[[{\"firstName\":\"Eddie\",\"lastName\":" +
      "\"Larrison\",\"conformedName\":\"LARRISON\"}],[{\"firstName\":\"Scarlett\",\"lastName\":\"Johanson\"," +
      "\"conformedName\":\"JOHANSON\"},{\"firstName\":\"William\",\"lastName\":\"Windows\",\"conformedName\":\"WINDOWS\"}]]}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  test("Test arrays struct containing an array of struct") {
    import spark.implicits._

    // Array of struct
    val df = spark.sparkContext.parallelize(
      Seq(
        Trade(1, Seq(
          Leg(100, Seq(
            Condition("if bid>10", "buy", 100), Condition("if sell<5", "sell", 150), Condition("if sell<1", "sell", 1000))),
          Leg(101, Seq(
            Condition("if bid<50", "sell", 200), Condition("if sell>30", "buy", 175), Condition("if sell>25", "buy", 225)))
        )),
        Trade(2, Seq(
          Leg(102, Seq(
            Condition("if bid>11", "buy", 100), Condition("if sell<6", "sell", 150), Condition("if sell<2", "sell", 1000))),
          Leg(103, Seq(
            Condition("if bid<51", "sell", 200), Condition("if sell>31", "buy", 175), Condition("if sell>26", "buy", 225)))
        )),
        Trade(3, Seq(
          Leg(104, Seq(
            Condition("if bid>12", "buy", 100), Condition("if sell<7", "sell", 150), Condition("if sell<3", "sell", 1000))),
          Leg(105, Seq(
            Condition("if bid<52", "sell", 200), Condition("if sell>32", "buy", 175), Condition("if sell>27", "buy", 225)))
        ))
      )).toDF

    //df.printSchema()
    //df.toJSON.take(10).foreach(println)

    val dfOut = ArrayTransformations.nestedWithColumnMap(df, "legs.conditions.conif", "conformedField", c => {
      upper(c)
    })

    val actualSchema = dfOut.schema.treeString
    val actualResults = dfOut.toJSON.collect.mkString("\n")

    val expectedSchema = """root
                           | |-- id: integer (nullable = false)
                           | |-- legs: array (nullable = true)
                           | |    |-- element: struct (containsNull = false)
                           | |    |    |-- legid: integer (nullable = true)
                           | |    |    |-- conditions: array (nullable = true)
                           | |    |    |    |-- element: struct (containsNull = false)
                           | |    |    |    |    |-- conif: string (nullable = true)
                           | |    |    |    |    |-- conthen: string (nullable = true)
                           | |    |    |    |    |-- amount: double (nullable = true)
                           | |    |    |    |    |-- conformedField: string (nullable = true)
                           |""".stripMargin
    val expectedResults = "{\"id\":1,\"legs\":[{\"legid\":100,\"conditions\":[{\"conif\":\"if bid>10\",\"conthen\":\"buy\"," +
      "\"amount\":100.0,\"conformedField\":\"IF BID>10\"},{\"conif\":\"if sell<5\",\"conthen\":\"sell\",\"amount\":150.0," +
      "\"conformedField\":\"IF SELL<5\"},{\"conif\":\"if sell<1\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedField\"" +
      ":\"IF SELL<1\"}]},{\"legid\":101,\"conditions\":[{\"conif\":\"if bid<50\",\"conthen\":\"sell\",\"amount\":200.0," +
      "\"conformedField\":\"IF BID<50\"},{\"conif\":\"if sell>30\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedField\"" +
      ":\"IF SELL>30\"},{\"conif\":\"if sell>25\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedField\":\"IF SELL>25\"}]}]}" +
      "\n{\"id\":2,\"legs\":[{\"legid\":102,\"conditions\":[{\"conif\":\"if bid>11\",\"conthen\":\"buy\",\"amount\":100.0," +
      "\"conformedField\":\"IF BID>11\"},{\"conif\":\"if sell<6\",\"conthen\":\"sell\",\"amount\":150.0,\"conformedField\":" +
      "\"IF SELL<6\"},{\"conif\":\"if sell<2\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedField\":\"IF SELL<2\"}]}," +
      "{\"legid\":103,\"conditions\":[{\"conif\":\"if bid<51\",\"conthen\":\"sell\",\"amount\":200.0,\"conformedField\":" +
      "\"IF BID<51\"},{\"conif\":\"if sell>31\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedField\":\"IF SELL>31\"}," +
      "{\"conif\":\"if sell>26\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedField\":\"IF SELL>26\"}]}]}\n{\"id\":3," +
      "\"legs\":[{\"legid\":104,\"conditions\":[{\"conif\":\"if bid>12\",\"conthen\":\"buy\",\"amount\":100.0," +
      "\"conformedField\":\"IF BID>12\"},{\"conif\":\"if sell<7\",\"conthen\":\"sell\",\"amount\":150.0,\"conformedField\":" +
      "\"IF SELL<7\"},{\"conif\":\"if sell<3\",\"conthen\":\"sell\",\"amount\":1000.0,\"conformedField\":\"IF SELL<3\"}]}," +
      "{\"legid\":105,\"conditions\":[{\"conif\":\"if bid<52\",\"conthen\":\"sell\",\"amount\":200.0,\"conformedField\":" +
      "\"IF BID<52\"},{\"conif\":\"if sell>32\",\"conthen\":\"buy\",\"amount\":175.0,\"conformedField\":\"IF SELL>32\"}," +
      "{\"conif\":\"if sell>27\",\"conthen\":\"buy\",\"amount\":225.0,\"conformedField\":\"IF SELL>27\"}]}]}"

    assertSchema(actualSchema, expectedSchema)
    assertResults(actualResults, expectedResults)
  }

  private def assertSchema(actualSchema: String, expectedSchema: String): Unit = {
    if (actualSchema != expectedSchema) {
      println("EXPECTED:")
      println(expectedSchema)
      println("ACTUAL:")
      println(actualSchema)
      fail("Actual conformed schema does not match the expected schema (see above).")
    }
  }

  private def assertResults(actualResults: String, expectedResults: String): Unit = {
    if (actualResults != expectedResults) {
      println("EXPECTED:")
      println(expectedResults)
      println("ACTUAL:")
      println(actualResults)
      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }
  }
}

