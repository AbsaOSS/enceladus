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

package za.co.absa.enceladus.utils.performance

import java.io.PrintWriter
import java.time.{Duration, ZonedDateTime}

class PerformanceMeasurer(val jobName: String) {
  private var startDateTime: Option[ZonedDateTime] = None
  private var finishDateTime: Option[ZonedDateTime] = None
  private var inputDatasetSize: Option[Long] = None
  private var outputDatasetSize: Option[Long] = None
  private var numberOfRecords: Option[Long] = None

  class PerfMetricsBuilder (val content: String = "") {
    def addVariable(name: String, value: Any): PerfMetricsBuilder = {
      new PerfMetricsBuilder(content + s"""${name.toLowerCase()}="$value"\n""")
    }
    def getMetrics: String = content
  }

  implicit def MetricsToString(metrics: PerfMetricsBuilder): String = metrics.getMetrics

  def startMeasurement(inputDatasetSize: Long): Unit = {
    startDateTime = Some(ZonedDateTime.now())
    this.inputDatasetSize = Some(inputDatasetSize)
  }

  def finishMeasurement(outputDatasetSize: Long, numberOfRecords: Long): Unit = {
    finishDateTime = Some(ZonedDateTime.now())
    this.outputDatasetSize = Some(outputDatasetSize)
    this.numberOfRecords = Some(numberOfRecords)
  }

  def getPerformanceMetricsText: String = {
    val elapsedTime = (startDateTime, finishDateTime) match {
      case (Some(start), Some(finish)) => Duration.between(start, finish)
      case _ => throw new IllegalStateException(s"Performance metrics: either start or finish time is not set ($startDateTime .. $finishDateTime).")
    }
    new PerfMetricsBuilder()
      .addVariable("perf_job_name", jobName)
      .addVariable("perf_start_date_time", startDateTime.get)
      .addVariable("perf_finish_date_time", finishDateTime.get)
      .addVariable("perf_input_dataset_size", inputDatasetSize.get)
      .addVariable("perf_output_dataset_size", outputDatasetSize.get)
      .addVariable("perf_num_of_records", numberOfRecords.get)
      .addVariable("perf_elapsed_seconds", elapsedTime.getSeconds)
  }

  def writeMetricsToFile(fileName: String): Unit = {
    val writer = new PrintWriter(fileName)
    try {
      writer.write(getPerformanceMetricsText)
    } finally {
      writer.close()
    }
  }

}
