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

package za.co.absa.enceladus.model.menas.scheduler

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import za.co.absa.enceladus.model.menas.jackson._

package object dataFormats {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
  @JsonSubTypes(Array(new Type(value = classOf[XMLDataFormat], name = "xml"),
    new Type(value = classOf[CSVDataFormat], name = "csv"),
    new Type(value = classOf[ParquetDataFormat], name = "parquet"),
    new Type(value = classOf[FixedWidthDataFormat], name = "fixed-width"),
    new Type(value = classOf[JSONFormat], name = "json")))
  sealed trait DataFormat {
    val name: String
    def getArguments: Seq[String]
  }

  case class XMLDataFormat(rowTag: String) extends DataFormat {
    val name: String = "xml"
    override def getArguments: Seq[String] = Seq("--row-tag", rowTag)
  }

  case class CSVDataFormat(@JsonDeserialize(using = classOf[OptCharDeserializer]) csvDelimiter: Option[Char],
      @JsonDeserialize(using = classOf[NonNullOptBooleanDeserializer]) csvHeader: Option[Boolean]) extends DataFormat {
    val name: String = "csv"
    private val delimiter = csvDelimiter.map(d => Seq("--delimiter", d.toString)).getOrElse(Seq[String]())
    private val header = csvHeader.map(h => Seq("--header", h.toString)).getOrElse(Seq[String]())
    override def getArguments: Seq[String] = (delimiter ++ header)
  }

  case class ParquetDataFormat() extends DataFormat {
    val name: String = "parquet"
    override def getArguments: Seq[String] = Seq()
  }

  case class FixedWidthDataFormat(@JsonDeserialize(using = classOf[NonNullOptBooleanDeserializer]) trimValues: Option[Boolean]) extends DataFormat {
    val name: String = "fixed-width"
    private val trim = trimValues.map(t => Seq("--trimValues", t.toString)).getOrElse(Seq[String]())
    override def getArguments: Seq[String] = trim
  }

  case class JSONFormat() extends DataFormat {
    val name: String = "json"
    override def getArguments: Seq[String] = Seq()
  }
}
