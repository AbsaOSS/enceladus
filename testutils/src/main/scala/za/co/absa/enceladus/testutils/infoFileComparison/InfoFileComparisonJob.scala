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

package za.co.absa.enceladus.testutils.infoFileComparison

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.ControlMeasuresLoaderJsonFile
import za.co.absa.atum.utils.ARMImplicits
import za.co.absa.enceladus.testutils.exceptions.InfoFilesDifferException
import za.co.absa.enceladus.testutils.infoFileComparison.AtumModelUtils._
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

object InfoFileComparisonJob {
  private val log: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    val enableWholeStage = false //disable whole stage code gen - the plan is too long

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(s"_INFO file comparison - '${cmd.newPath}' and '${cmd.refPath}'")
      .config("spark.sql.codegen.wholeStage", enableWholeStage)
      .getOrCreate()
    TimeZoneNormalizer.normalizeAll(Seq(sparkSession))

    implicit val sc: SparkContext = sparkSession.sparkContext
    val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration

    val newPathLoader = new ControlMeasuresLoaderJsonFile(hadoopConfiguration, new Path(cmd.newPath))
    val refPathLoader = new ControlMeasuresLoaderJsonFile(hadoopConfiguration, new Path(cmd.refPath))

    val newControlMeasure: ControlMeasure = newPathLoader.load()
    val refControlMeasure: ControlMeasure = refPathLoader.load()

    val diff: List[ModelDifference[_]] = refControlMeasure.compareWith(newControlMeasure)

    if (diff.nonEmpty){
      val path = new Path(cmd.outPath)
      val serializedData: String = ModelDifferenceParser.asJson(diff)

      saveDataToFile(serializedData, path, hadoopConfiguration)

      throw InfoFilesDifferException(cmd.refPath, cmd.newPath, cmd.outPath)
    } else {
      log.info("Expected and actual _INFO files are the same.")
    }
  }

  private[this] def saveDataToFile(data: String, path: Path, hadoopConfiguration: Configuration): Unit = {
    import ARMImplicits._
    val fs = FileSystem.get(hadoopConfiguration)
    val overwrite = true
    val progress = null
    val permission = new FsPermission("777")
    val bufferSize = hadoopConfiguration.getInt("io.file.buffer.size", 4096)

    for (fos <- fs.create(
      path,
      permission,
      overwrite,
      bufferSize,
      fs.getDefaultReplication(path),
      fs.getDefaultBlockSize(path),
      progress)
    ){
      fos.write(data.getBytes)
    }
  }
}
