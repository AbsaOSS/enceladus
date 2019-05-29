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

import java.io.FileInputStream

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.atum.persistence.ControlMeasuresParser
import za.co.absa.atum.utils.ARMImplicits
import za.co.absa.enceladus.testutils.exceptions.InfoFilesDifferException
import za.co.absa.enceladus.testutils.infoFileComparison.AtumModelUtils._

import scala.collection.JavaConverters._
import scala.reflect.io.File

object InfoFileComparisonJob {
  private val log: Logger = LogManager.getLogger(this.getClass)
  private lazy val hadoopConfiguration = getHadoopConfiguration

  def main(args: Array[String]): Unit = {
    val cmd = CmdConfig.getCmdLineArguments(args)

    val newControlMeasure = loadControlMeasures(cmd.newPath)
    val refControlMeasure = loadControlMeasures(cmd.refPath)

    val diff: List[ModelDifference[_]] = refControlMeasure.compareWith(newControlMeasure)

    if (diff.nonEmpty){
      val serializedData: String = ModelDifferenceParser.asJson(diff)

      saveDataToFile(serializedData, cmd.outPath)

      throw InfoFilesDifferException(cmd.refPath, cmd.newPath, cmd.outPath)
    } else {
      log.info("Expected and actual _INFO files are the same.")
    }
  }

  private[this] def saveDataToFile(data: String, path: String): Unit = {
    path match {
      case p if p.startsWith("file://") => File(p.stripPrefix("file://")).writeAll(data)
      case p => saveDataToHDFSFile(data, new Path(p))
    }
  }


  private[this] def saveDataToHDFSFile(data: String, path: Path): Unit = {
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

  private[this] def loadControlMeasures(path: String): ControlMeasure = {
    val stream = path match {
      case p if p.startsWith("file://") => new FileInputStream(p.stripPrefix("file://"))
      case p => FileSystem.get(hadoopConfiguration).open(new Path(p))
    }
    val controlInfoJson = try IOUtils.readLines(stream).asScala.mkString("\n") finally stream.close()
    ControlMeasuresParser.fromJson(controlInfoJson)
  }
}
