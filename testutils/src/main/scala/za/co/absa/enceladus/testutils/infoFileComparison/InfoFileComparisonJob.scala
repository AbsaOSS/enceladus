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
import za.co.absa.atum.persistence.ControlMeasuresLoaderJsonFile
import za.co.absa.atum.utils.ARMImplicits
import za.co.absa.enceladus.testutils.exceptions.InfoFilesDifferException
import za.co.absa.enceladus.testutils.infoFileComparison.AtumModelUtils._

object InfoFileComparisonJob {
  private val log: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val cmd = CmdConfig.getCmdLineArguments(args)

    val hadoopConfiguration = getHadoopConfiguration

    val newPathLoader = new ControlMeasuresLoaderJsonFile(hadoopConfiguration, new Path(cmd.newPath))
    val refPathLoader = new ControlMeasuresLoaderJsonFile(hadoopConfiguration, new Path(cmd.refPath))

    val newControlMeasure = newPathLoader.load()
    val refControlMeasure = refPathLoader.load()

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

  private[this] def getHadoopConfiguration: Configuration = {
    val hadoopConfDir = sys.env("HADOOP_CONF_DIR")
    val coreSiteXmlPath = s"$hadoopConfDir/core-site.xml"
    val hdfsSiteXmlPath = s"$hadoopConfDir/hdfs-site.xml"
    val conf = new Configuration()
    conf.clear()

    conf.addResource(new Path(coreSiteXmlPath))
    conf.addResource(new Path(hdfsSiteXmlPath))
    conf
  }
}
