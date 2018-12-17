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

package za.co.absa

import java.io._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import scala.util.Random
import Numeric.Implicits._

/* Generate data for testing
 * Data contains a given number of columns and rows
 * Coulmns are of type: string, int, date, float, boolean
 * First 6 columns have predefined names and types, the rest are repeated using a pattern
 * Data are generated at random or using simple rules (eg x = row * 100000 + col})
 */

object RandomDataGenerator {

  // Set DateTime format to be used
  val dtf: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")

  // Predefined columns
  val reservedColumns: Map[Int,String] = Map(
    1 -> "key",
    2 -> "id",
    3 -> "version",
    4 -> "Date",
    5 -> "value",
    6 -> "address")

  // Initialize random generator
  private val random = new Random(System.nanoTime)

  private def generateRecord(row: Int, col: Int): String = {
    // if predifined column
    if ( reservedColumns.contains(col) ) {
      reservedColumns(col) match {
        case "key" => row.toString()
        case "id" => random.nextInt(10).toString
        case "Date" => (new DateTime)
          .withYear(2000 + random.nextInt(17))
          .withMonthOfYear(1 + random.nextInt(11))
          .withDayOfMonth(1 + random.nextInt(27))
          .withHourOfDay(3 + random.nextInt(20))
          .withMinuteOfHour(random.nextInt(60))
          .withSecondOfMinute(random.nextInt(60))
          .toString(dtf)
        case "version" => random.nextInt(3).toString
        case "value"   => (1000 * random.nextFloat()).toDouble.toString
        case "address" => Random.alphanumeric.take(10).mkString("")
      }
    } else { // rest of columns
      col % 5 match {
        case 0 => s"r${row}c${col}" // String pattern
        case 1 => s"${row * 100000 + col}" // int pattern
        case 2 => (new DateTime) // random date
          .withYear(2000 + random.nextInt(2017))
          .withMonthOfYear(1 + random.nextInt(11))
          .withDayOfMonth(1 + random.nextInt(27))
          .withHourOfDay(3 + random.nextInt(20))
          .withMinuteOfHour(random.nextInt(60))
          .withSecondOfMinute(random.nextInt(60))
          .toString(dtf)
        case 3 => {row + col * 0.000001}.toDouble.toString  // float pattern
        case 4 => random.nextBoolean().toString // random boolean
      }
    }
  }

  // Generate colums names indicating types
  private def generateHeaderRecord(col: Int): String = {
    if ( reservedColumns.contains(col) ) {
      reservedColumns(col)
    } else {
      col % 5 match {
        case 0 => s"col${col}_string"
        case 1 => s"col${col}_int"
        case 2 => s"col${col}_date"
        case 3 => s"col${col}_float"
        case 4 => s"col${col}_boolean"
      }
    }
  }

  // Generate and write data to a CSV file with a given number of columns and rows
  // Output file: pathPrefixgeneratedData_X-records_Y-columns_Z-rows.csv
  def generateCSVfile(pathPrefix: String, columns: Int, rows: Int): Unit = {
    val delimiter = ","
    val totalRecords = rows * columns
    val filename = s"${pathPrefix}generatedData_${totalRecords}-records_${columns}-columns_${rows}-rows.csv"
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))

    // write header
    for (c <- 1 to columns){
      bw.write(generateHeaderRecord(c))
      if (c == columns) {
        // last column
        bw.write("\n")
      }else{
        // column in the middle
        bw.write(delimiter)
      }
    }

    // write rows
    for (r <- 1 to rows) {
      for (c <- 1 to columns) {
        bw.write(generateRecord(r, c))
        if (c == columns) {
          // last column
          bw.write("\n")
        }else{
          // column in the middle
          bw.write(delimiter)
        }
      }
    }
    bw.close()
    println(s"File witten: ${filename}")

  }

  def main(args: Array[String]): Unit = {
    val columnsSeq = Seq(100, 200, 400)
    val path = s"/Users/abdmac7/tmp/"

    for (col <- columnsSeq) {
      generateCSVfile(path, col, 2560000/col )
    }

  }
}
