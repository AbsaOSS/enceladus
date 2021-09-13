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

package za.co.absa.enceladus.conformance.interpreter.fixtures

import org.apache.commons.configuration2.Configuration
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row}
import org.mockito.Mockito.lenient
import org.scalatest.funsuite.AnyFunSuite
import org.mockito.scalatest.MockitoSugar
import za.co.absa.enceladus.conformance.HyperConformance
import za.co.absa.enceladus.conformance.HyperConformanceAttributes._
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.FeatureSwitches
import za.co.absa.enceladus.conformance.streaming.{InfoDateFactory, InfoVersionFactory}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

trait StreamingFixture extends AnyFunSuite with SparkTestBase with MockitoSugar {
  implicit val menasBaseUrls: List[String] = List.empty
  implicit val cmd: ConformanceConfig = ConformanceConfig(reportVersion = Some(1), reportDate = "2020-03-23")

  protected def testHyperConformanceFromConfig(input: DataFrame,
                                               sinkTableName: String,
                                               dataset: Dataset,
                                               reportDate: String)
                                              (implicit menasDAO: MenasDAO): DataFrame = {
    val configStub: Configuration = mock[Configuration]
    when(configStub.containsKey(reportVersionKey)).thenReturn(false)
    when(configStub.containsKey(eventTimestampColumnKey)).thenReturn(false)
    lenient.when(configStub.containsKey(reportVersionColumnKey)).thenReturn(true)
    when(configStub.getString(reportVersionColumnKey)).thenReturn("numerics.SmartObject.all_random")
    when(configStub.containsKey(reportDateKey)).thenReturn(true)
    when(configStub.getString(reportDateKey)).thenReturn(reportDate)
    when(configStub.containsKey(datasetNameKey)).thenReturn(true)
    when(configStub.getString(datasetNameKey)).thenReturn("StreamingDataset")
    when(configStub.containsKey(datasetVersionKey)).thenReturn(true)
    when(configStub.getInt(datasetVersionKey)).thenReturn(1)
    when(configStub.containsKey(menasUriKey)).thenReturn(true)
    when(configStub.getString(menasUriKey)).thenReturn("https://mymenas.org")
    when(configStub.containsKey(menasAuthKeytabKey)).thenReturn(true)
    when(configStub.containsKey(menasCredentialsFileKey)).thenReturn(false)
    when(configStub.getString(menasAuthKeytabKey)).thenReturn("key1")

    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(input.schema))
    val hyperConformance = HyperConformance(configStub).asInstanceOf[HyperConformance]
    val source: DataFrame = memoryStream.toDF()
    val conformed: DataFrame = hyperConformance.applyConformanceTransformations(source, dataset)
    val sink = conformed
      .writeStream
      .queryName(sinkTableName)
      .outputMode("append")
      .format("memory")
      .start()

    input.collect().foreach(e => {
      memoryStream.addData(e)
      sink.processAllAvailable()
    })

    val frame: DataFrame = spark.sql(s"select * from $sinkTableName")

    sink.stop()
    frame
  }

  protected def testHyperConformance(input: DataFrame,
                                     sinkTableName: String,
                                     dataset: Dataset,
                                     catalystWorkaround: Boolean = true)
                                    (implicit menasDAO: MenasDAO, infoDateFactory: InfoDateFactory,
                                     infoVersionFactory: InfoVersionFactory): DataFrame = {
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(false)
      .setCatalystWorkaroundEnabled(catalystWorkaround)
      .setControlFrameworkEnabled(false)

    val memoryStream = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(input.schema))
    val hyperConformance = new HyperConformance()
    val source: DataFrame = memoryStream.toDF()
    val conformed: DataFrame = hyperConformance.applyConformanceTransformations(source, dataset)
    val sink = conformed
      .writeStream
      .queryName(sinkTableName)
      .outputMode("append")
      .format("memory")
      .start()

    input.collect().foreach(e => {
      memoryStream.addData(e)
      sink.processAllAvailable()
    })

    val frame: DataFrame = spark.sql(s"select * from $sinkTableName")

    sink.stop()
    frame
  }
}
