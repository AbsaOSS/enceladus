//todo reenable with Hyperdrive API for Scala 2.12 #1712
//
///*
// * Copyright 2018 ABSA Group Limited
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package za.co.absa.enceladus.conformance.streaming
//
//import org.apache.commons.configuration2.Configuration
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{DateType, StringType, TimestampType}
//import org.apache.spark.sql.{Column, DataFrame}
//import org.slf4j.{Logger, LoggerFactory}
//import za.co.absa.enceladus.common.Constants.ReportDateFormat
//import za.co.absa.enceladus.conformance.datasource.PartitioningUtils
//import za.co.absa.enceladus.utils.schema.SchemaUtils
//
///**
// * Info date factory allows to create an expression for the information date based on the strategy used.
// * It is one of:
// * - Have a fixed information date provided by the configuration (or Hyperdrive command line)
// * - Derive it from an event timestamp column
// * - Use the processing time
// */
//sealed trait InfoDateFactory {
//  def getInfoDateColumn(df: DataFrame): Column
//}
//
//class InfoDateLiteralFactory(infoDate: String) extends InfoDateFactory {
//  override def getInfoDateColumn(df: DataFrame): Column = {
//    PartitioningUtils.validateReportDate(infoDate)
//    to_date(lit(infoDate), ReportDateFormat)
//  }
//}
//
//class InfoDateFromColumnFactory(columnName: String, pattern: String) extends InfoDateFactory {
//  override def getInfoDateColumn(df: DataFrame): Column = {
//    val dt = SchemaUtils.getFieldType(columnName, df.schema)
//    dt match {
//      case Some(TimestampType) =>
//        col(columnName).cast(DateType)
//      case Some(DateType) =>
//        col(columnName)
//      case Some(StringType) =>
//        to_timestamp(col(columnName), pattern).cast(DateType)
//      case Some(_) =>
//        throw new IllegalArgumentException(s"The specified event time column $columnName has an incompatible type: $dt")
//      case None =>
//        throw new IllegalArgumentException(s"The specified event time column does not exist: $columnName")
//    }
//  }
//}
//
//class InfoDateFromProcessingTimeFactory extends InfoDateFactory {
//  override def getInfoDateColumn(df: DataFrame): Column = current_timestamp().cast(DateType)
//}
//
//object InfoDateFactory {
//  import za.co.absa.enceladus.conformance.HyperConformanceAttributes._
//
//  private val defaultEventTimestampPattern = "yyyy-MM-dd'T'HH:mm'Z'"
//
//  val log: Logger = LoggerFactory.getLogger(this.getClass)
//
//  def getFactoryFromConfig(conf: Configuration): InfoDateFactory = {
//    if (conf.containsKey(reportDateKey)) {
//      val reportDate = conf.getString(reportDateKey)
//      log.info(s"Information date: Explicit from the job configuration = $reportDate")
//      new InfoDateLiteralFactory(reportDate)
//    } else if (conf.containsKey(eventTimestampColumnKey)) {
//      val eventTimestampColumnName = conf.getString(eventTimestampColumnKey)
//      val eventTimestampPattern = conf.getString(eventTimestampPatternKey, defaultEventTimestampPattern)
//      log.info(s"Information date: Derived from the event time column = $eventTimestampColumnName")
//      new InfoDateFromColumnFactory(eventTimestampColumnName, eventTimestampPattern)
//    } else {
//      log.info(s"Information date: Processing time is used")
//      new InfoDateFromProcessingTimeFactory()
//    }
//  }
//}
