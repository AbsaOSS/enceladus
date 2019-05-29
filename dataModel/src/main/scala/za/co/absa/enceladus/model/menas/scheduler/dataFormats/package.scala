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

package za.co.absa.enceladus.model.menas.scheduler

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

package object dataFormats {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
  @JsonSubTypes(Array(new Type(value = classOf[XMLDataFormat], name = "xml"),
    new Type(value = classOf[CSVDataFormat], name = "csv"),
    new Type(value = classOf[ParquetDataFormat], name = "parquet"),
    new Type(value = classOf[FixedWidthDataFormat], name = "fixed-width")))
  sealed trait DataFormat {
    val name: String
    def getArguments: Seq[String]
  }

  case class XMLDataFormat(rowTag: String) extends DataFormat {
    val name = "xml"
    override def getArguments() = Seq("--row-tag", rowTag)
  }

  case class CSVDataFormat(csvDelimiter: Option[String], csvHeader: Option[Boolean]) extends DataFormat {
    val name = "csv"
    val delimiter = csvDelimiter.flatMap(d => if(d.isEmpty()) None else Some(Seq("--delimiter", d))).getOrElse(Seq[String]())
    val header = csvHeader.map(h => Seq("--header", h.toString)).getOrElse(Seq[String]())
    override def getArguments() = (delimiter ++ header).filter(!_.isEmpty)
  }

  case class ParquetDataFormat() extends DataFormat {
    val name = "parquet"
    override def getArguments() = Seq()
  }

  case class FixedWidthDataFormat(trimValues: Option[Boolean]) extends DataFormat {
    val name = "fixed-width"
    val trim = trimValues.map(t => Seq("--trimValues", t.toString)).getOrElse(Seq[String]())
    override def getArguments() = trim.filter(!_.isEmpty)
  }
}
