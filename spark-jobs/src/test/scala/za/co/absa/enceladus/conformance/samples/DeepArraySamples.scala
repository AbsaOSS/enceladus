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

import org.apache.spark.sql.types._

object DeepArraySamples {

  // Orders example
  case class Payment(payid: String, amount: Double)

  case class OrderItem(itemid: String, qty: Int, price: Double, payments: Seq[Payment])

  case class Order(id: Long, name: String, items: Seq[OrderItem])

  val ordersData: Seq[Order] = Seq[Order](
    Order(1L, "First Order", Seq[OrderItem](
      OrderItem("ar229", 10, 5.1, Seq(Payment("pid10", 51.0))),
      OrderItem("2891k", 100, 1.1, Seq(Payment("zk20", 100.0))),
      OrderItem("31239", 2, 55.2, Nil)
    )),
      Order(2L, "Second Order", Seq[OrderItem](
      OrderItem("AkuYdg", 100, 10, Seq(Payment("d101", 10.0), Payment("d102", 20.0))),
      OrderItem("jUa1k0", 2, 55.2, Nil)
    )),
      Order(3L, "Third Order", Seq[OrderItem](
      OrderItem("Gshj1", 10, 10000, Seq(Payment("pid10", 2000.0), Payment("pid10", 5000.0))),
      OrderItem("Jdha2", 100, 45, Seq(Payment("zk20", 150.0), Payment("pid10", 2000.0)))
    )),
      Order(4L, "Fourth Order", Seq[OrderItem](
      OrderItem("dLda1", 10, 5.1, Seq(Payment("pid10", 10.0))),
      OrderItem("d2dhJ", 100, 1.1, Seq(Payment("zk20", 15.0))),
      OrderItem("Mska0", 2, 55.2, Nil),
      OrderItem("Gdal1", 20, 5.2, Nil),
      OrderItem("dakl1", 99, 1.2, Nil)
    )),
      Order(5L, "Fifths order", Seq[OrderItem](
      OrderItem("hdUs1J", 50, 0.2, Seq(Payment("pid10", 10.0), Payment("pid10", 11.0), Payment("pid10", 12.0)))
    ))
  )

  val ordersDataWithNulls: Seq[Order] = Seq[Order](
    Order(1L, "First Order", Seq[OrderItem](
      OrderItem("ar229", 10, 5.1, Seq(Payment("pid10", 51.0))),
      OrderItem("2891k", 100, 1.1, Seq(Payment("zk20", 100.0))),
      OrderItem("31239", 2, 55.2, Nil)
    )),
    Order(2L, null, Seq[OrderItem](
      OrderItem("AkuYdg", 100, 10, Seq(Payment("d101", 10.0), Payment("d102", 20.0))),
      OrderItem("jUa1k0", 2, 55.2, Nil)
    )),
    Order(3L, "Third Order", Seq[OrderItem](
      OrderItem(null, 10, 10000, Seq(Payment("pid10", 2000.0), Payment("pid10", 5000.0))),
      OrderItem("Jdha2", 100, 45, Seq(Payment("zk20", 150.0), Payment("pid10", 2000.0)))
    )),
    Order(4L, "Fourth Order", Seq[OrderItem](
      OrderItem("dLda1", 10, 5.1, Seq(Payment("pid10", 10.0))),
      OrderItem("d2dhJ", 100, 1.1, Seq(Payment("zk20", 15.0))),
      OrderItem("Mska0", 2, 55.2, Nil),
      OrderItem("Gdal1", 20, 5.2, Nil),
      OrderItem("dakl1", 99, 1.2, Nil)
    )),
    Order(5L, "Fifths order", Seq[OrderItem](
      OrderItem("hdUs1J", 50, 0.2, Seq(Payment("pid10", 10.0), Payment("pid10", 11.0), Payment("pid10", 12.0)))
    ))
  )

  val ordersSchema = StructType(
    Array(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("items", ArrayType(StructType(Array(
        StructField("itemid", StringType, nullable = false),
        StructField("qty", IntegerType, nullable = false),
        StructField("price", DecimalType(18, 10), nullable = false),
        StructField("payments", ArrayType(StructType(Array(
          StructField("payid", StringType, nullable = false),
          StructField("amount", DecimalType(18, 10), nullable = false)
        )))) // payments
      )))) // items
    ))

  // Library example
  case class Book(author: String, name: String)

  case class LibraryRoom(roomName: String, label: String, capacity: Int, books: Seq[Book])

  case class Library(id: Long, name: String, city: String, address: String, rooms: Seq[LibraryRoom])

  val libraryData: Seq[Library] = Seq(
    Library(1, "National Library", "Washington", "10 Linkoln ave.", Seq(
      LibraryRoom("History", "H1", 25000, Seq(Book("Christopher Browning", "Ordinary Men"), Book("Vasily Grossman", "Life and Fate"), Book("Homer", "Illiad"))),
      LibraryRoom("Politics", "P2", 15000, Seq(Book("Michael Lewis", "The Fifths Risk"), Book("Aristotle", "Politics"))),
      LibraryRoom("Sociology", "S3", 8000, Seq())
    )),
    Library(2, "Technical Library", "New York", "101/2 Park ave.", Seq(
      LibraryRoom("Math", "M41", 10000, Seq(Book("C. C. Tisdell", "Introduction to Complex Numbers"), Book("Michael Batty", "Essential Engineering Mathematics"), Book("Gareth J. Janecek", "Mathematics for Computer Scientist"))),
      LibraryRoom("Physics", "P5", 15000, Seq(Book("Ali R. Fazely", "Foundation of Physics for Scientists and Engineers"), Book("Tarik Al-Shemmeri", "Engineering Thermodynamics"), Book("Satindar Bhagat", "Elementary Physics I"))),
      LibraryRoom("IT", "I6", 35000, Seq(Book("Garry Turkington", "Hadoop Beginner’s Guide"), Book("Thomas H. Davenport", "Big Data at Work"), Book("Martin Kleppmann", "Designing Data-Intensive Applications")))
    )),
    Library(3, "Prazska Knihovna", "Prague", "Vikova 1223/4", Seq(
      LibraryRoom("Literature", "L7", 55000, Seq(Book("James Joyce", "Ulysses"), Book("Herman Melville", "Moby Dick"), Book("William Shakespeare", "Hamlet"))),
      LibraryRoom("Poetry", "P8", 35000, Seq(Book("Simon Armitage", "The Unaccompanied"), Book("D. Nurkse", "Love in the Last Days"), Book("Federico García Lorca", "Poet in Spain")))
    ))
  )

}
