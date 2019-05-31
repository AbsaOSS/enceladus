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

package za.co.absa.enceladus.menas.repositories

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import org.apache.oozie.client.OozieClient
import scala.concurrent.Future
import org.apache.oozie.client.WorkflowJob.{Status => WorkflowStatus}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import org.apache.hadoop.conf.Configuration
import java.util.Properties
import za.co.absa.enceladus.model.Dataset
import org.springframework.beans.factory.annotation.Value
import za.co.absa.enceladus.menas.exceptions.OozieConfigurationException
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import java.util.TimeZone
import org.apache.oozie.client.Job.Status
import za.co.absa.enceladus.menas.models.OozieCoordinatorStauts
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.permission.FsAction
import za.co.absa.enceladus.model.menas.scheduler.RuntimeConfig
import java.net.URL
import java.net.HttpURLConnection
import org.springframework.beans.factory.InitializingBean
import org.slf4j.LoggerFactory

@Repository
class OozieRepository @Autowired() (oozieClientRes: Either[OozieConfigurationException, OozieClient], datasetMongoRepository: DatasetMongoRepository,
    hadoopFS: FileSystem, hadoopConf: Configuration) extends InitializingBean {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.collection.JavaConversions._

  @Value("${za.co.absa.enceladus.menas.oozie.schedule.hdfs.path:}")
  val oozieScheduleHDFSPath: String = ""

  @Value("${za.co.absa.enceladus.menas.oozie.timeZone:Africa/Ceuta}")
  val oozieTimezone: String = ""

  @Value("${za.co.absa.enceladus.menas.oozie.sharelibForSpark:spark}")
  val oozieShareLib: String = ""

  @Value("${za.co.absa.enceladus.menas.oozie.enceladusJarLocation:}")
  val enceladusJarLocation: String = ""

  @Value("${za.co.absa.enceladus.menas.oozie.mavenStandardizationJarLocation:}")
  val standardizationJarPath: String = ""

  @Value("${za.co.absa.enceladus.menas.oozie.mavenConformanceJarLocation:}")
  val conformanceJarPath: String = ""

  @Value("${za.co.absa.enceladus.menas.oozie.mavenRepoLocation:}")
  val mavenRepoLocation: String = ""

  @Value("${za.co.absa.enceladus.menas.oozie.menasApiURL:}")
  val menasApiURL: String = ""

  @Value("${za.co.absa.enceladus.menas.oozie.splineMongoURL:}")
  val splineMongoURL: String = ""

  val classLoader = Thread.currentThread().getContextClassLoader()
  val workflowTemplate = getTemplateFile("scheduling/oozie/workflow_template.xml")
  val coordinatorTemplate = getTemplateFile("scheduling/oozie/coordinator_template.xml")
  val namenode = hadoopConf.get("fs.defaultFS")
  val resourceManager = hadoopConf.get("yarn.resourcemanager.address")
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
  val logger = LoggerFactory.getLogger(this.getClass)

  override def afterPropertiesSet() {
    logger.info(s"Enceladus Jar Location: $enceladusJarLocation")
    logger.info(s"Stanrdardization Jar Path: $standardizationJarPath")
    logger.info(s"Conformance Jar Path: $conformanceJarPath")
    //ensure that relevant jars are properly loaded in HDFS, otherwise initialize
    this.initializeJars()
  }

  private def validateProperties(): Boolean = {
    Seq((oozieScheduleHDFSPath, "za.co.absa.enceladus.menas.oozie.schedule.hdfs.path"),
      (enceladusJarLocation, "zza.co.absa.enceladus.menas.oozie.enceladusJarLocation"),
      (standardizationJarPath, "za.co.absa.enceladus.menas.oozie.mavenStandardizationJarLocation"),
      (conformanceJarPath, "za.co.absa.enceladus.menas.oozie.mavenConformanceJarLocation"),
      (mavenRepoLocation, "za.co.absa.enceladus.menas.oozie.mavenRepoLocation"),
      (menasApiURL, "za.co.absa.enceladus.menas.oozie.menasApiURL"),
      (splineMongoURL, "za.co.absa.enceladus.menas.oozie.splineMongoURL")).map(p => validateProperty(p._1, p._2)).reduce(_ && _)
  }

  private def validateProperty(prop: String, propName: String): Boolean = {
    if (prop == null || prop.isEmpty) {
      logger.warn(s"Oozie support disabled. Missing required configuration property $propName")
      false
    }
    true
  }

  private def initializeJars() {
    if (this.isOozieEnabled) {
      val hdfsStdPath = new Path(s"$enceladusJarLocation$standardizationJarPath")
      val hdfsConfPath = new Path(s"$enceladusJarLocation$conformanceJarPath")
      val mavenStdPath = s"$mavenRepoLocation$standardizationJarPath"
      val mavenConfPath = s"$mavenRepoLocation$conformanceJarPath"

      if (!hadoopFS.exists(hdfsStdPath) || hadoopFS.getStatus(hdfsStdPath).getCapacity == 0) {
        logger.info(s"Uploading standardization jar from $mavenStdPath to $hdfsStdPath")
        val resFuture = this.downloadFile(mavenStdPath, hdfsStdPath)
        resFuture.onSuccess({ case (u: Unit) => logger.info(s"Standardization jar loaded to $hdfsStdPath") })
        resFuture.onFailure({
          case (err: Throwable) =>
            hadoopFS.delete(hdfsStdPath, true)
        })
      }

      if (!hadoopFS.exists(hdfsConfPath) || hadoopFS.getStatus(hdfsConfPath).getCapacity == 0) {
        logger.info(s"Uploading conformance jar from $mavenConfPath to $hdfsConfPath")
        val resFuture = this.downloadFile(mavenConfPath, hdfsConfPath)
        resFuture.onSuccess({ case (u: Unit) => logger.info(s"Conformance jar loaded to $hdfsConfPath") })
        resFuture.onFailure({
          case (err: Throwable) =>
            hadoopFS.delete(hdfsConfPath, true)
        })
      }
    }
  }

  /**
   * Used for downloading the jar either from maven or local repo
   */
  private def downloadFile(url: String, hadoopPath: Path) = {
    Future {
      val connection = new URL(url).openConnection()
      connection match {
        case httpConn: HttpURLConnection => httpConn.setRequestMethod("GET")
        case _                           => Unit
      }

      val in = connection.getInputStream
      val targetArray = Array.fill(in.available)(0.toByte)
      in.read(targetArray)
      val os = hadoopFS.create(hadoopPath, true)
      os.write(targetArray)
      os.flush()
      os.close()
    }
  }

  /**
   * Read a template file packaged with the jar
   */
  private def getTemplateFile(fileName: String): String = {
    new BufferedReader(
      new InputStreamReader(
        classLoader.getResourceAsStream(fileName), "UTF-8")).lines().toArray().mkString("\n")
  }

  /**
   * Whether or not oozie is enabled/configured
   */
  def isOozieEnabled: Boolean = this.validateProperties() && oozieClientRes.isRight

  private def getOozieClient[T](fn: (OozieClient => Future[T])): Future[T] = {
    oozieClientRes match {
      case Right(client) => fn(client)
      case Left(ex)      => Future.failed(ex)
    }
  }

  private def getOozieClientWrap[T](fn: (OozieClient => T)): Future[T] = {
    getOozieClient({ cl: OozieClient =>
      Future(fn(cl))
    })
  }

  /**
   * Get status of submitted coordinater
   */
  def getCoordinatorStatus(coordId: String): Future[OozieCoordinatorStauts] = {
    getOozieClientWrap({ oozieClient: OozieClient =>
      val jobInfo = oozieClient.getCoordJobInfo(coordId)
      val nextMaterializeTime = dateFormat.format(jobInfo.getNextMaterializedTime)
      OozieCoordinatorStauts(jobInfo.getStatus, nextMaterializeTime)
    })
  }

  /**
   * Get status of a running workflow
   */
  def getJobStatus(jobId: String): Future[WorkflowStatus] = {
    getOozieClientWrap({ oozieClient: OozieClient =>
      oozieClient.getJobInfo(jobId).getStatus
    })
  }

  /**
   * Kill a running coordinator
   */
  def killCoordinator(coordId: String): Future[Unit] = {
    getOozieClientWrap({ oozieClient: OozieClient =>
      oozieClient.kill(coordId)
    })
  }

  /**
   * Get workflow from teplate - this fills in all variables and returns representation of the workflow
   */
  private def getWorkflowFromTemplate(ds: Dataset): Array[Byte] = {
    val schedule = ds.schedule.get
    val runtimeParams = schedule.runtimeParams
    workflowTemplate.replaceAll("\\$stdAppName", s"Menas Schedule Standardization ${ds.name} (${ds.version})")
      .replaceAll("\\$confAppName", s"Menas Schedule Conformance ${ds.name} (${ds.version})")
      .replaceAll("\\$stdJarPath", s"$enceladusJarLocation$standardizationJarPath")
      .replaceAll("\\$confJarPath", s"$enceladusJarLocation$conformanceJarPath")
      .replaceAll("\\$datasetVersion", schedule.datasetVersion.toString)
      .replaceAll("\\$datasetName", ds.name)
      .replaceAll("\\$mappingTablePattern", schedule.mappingTablePattern.flatMap(p => if (p.isEmpty()) None else Some(p)).getOrElse("reportDate={0}-{1}-{2}").trim)
      .replaceAll("\\$dataFormat", schedule.rawFormat.name)
      .replaceAll("\\$otherDFArguments", schedule.rawFormat.getArguments.map(arg => s"<arg>$arg</arg>").mkString("\n"))
      .replaceAll("\\$jobTracker", resourceManager)
      .replaceAll("\\$sharelibForSpark", oozieShareLib)
      .replaceAll("\\$nameNode", namenode)
      .replaceAll("\\$menasRestURI", menasApiURL)
      .replaceAll("\\$splineMongoURL", splineMongoURL)
      .replaceAll("\\$stdNumExecutors", runtimeParams.stdNumExecutors.toString)
      .replaceAll("\\$stdExecutorMemory", s"${runtimeParams.stdExecutorMemory}g")
      .replaceAll("\\$confNumExecutors", runtimeParams.confNumExecutors.toString)
      .replaceAll("\\$confExecutorMemory", s"${runtimeParams.confExecutorMemory}g")
      .replaceAll("\\$driverCores", s"${runtimeParams.driverCores}")
      .replaceAll("\\$menasCredentialsFile", s"$namenode${runtimeParams.menasCredentialFile}")
      .getBytes("UTF-8")
  }

  /**
   * Gets the coordinator from the template, filling in variables
   */
  private def getCoordinatorFromTemplate(ds: Dataset, wfPath: String): Array[Byte] = {
    val schedule = ds.schedule.get
    val runtimeParams = schedule.runtimeParams
    val currentTime = System.currentTimeMillis()
    val futureTime = currentTime + 3.1573e12.toLong
    val timezoneOffset = TimeZone.getTimeZone(oozieTimezone).getOffset(currentTime)
    val startDate = new Date(currentTime)
    val endDate = new Date(futureTime)
    coordinatorTemplate.replaceAll("\\$coordName", s"Menas Schedule Coordinator ${ds.name} (${ds.version})")
      .replaceAll("\\$cronTiming", schedule.scheduleTiming.getCronSchedule)
      .replaceAll("\\$reportDateOffset", schedule.reportDateOffset.toString)
      .replaceAll("\\$timezone", oozieTimezone)
      .replaceAll("\\$startDate", dateFormat.format(startDate))
      .replaceAll("\\$endDate", dateFormat.format(endDate))
      .replaceAll("\\$wfApplicationPath", wfPath).getBytes("UTF-8")
  }

  /**
   * Get oozie properties
   */
  private def getOozieConf(oozieClient: OozieClient, runtimeParams: RuntimeConfig): Properties = {
    val conf = oozieClient.createConfiguration()
    conf.setProperty("jobTracker", resourceManager)
    conf.setProperty("nameNode", namenode)
    conf.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "True")
    conf.setProperty("send_email", "False")
    conf.setProperty("mapreduce.job.user.name", runtimeParams.sysUser)
    conf.setProperty("security_enabled", "False")
    conf.setProperty("user.name", runtimeParams.sysUser)
    conf
  }

  /**
   * Submits a coordinator
   */
  def runCoordinator(coordPath: String, runtimeParams: RuntimeConfig): Future[String] = {
    getOozieClientWrap { oozieClient: OozieClient =>
      val conf = getOozieConf(oozieClient, runtimeParams)
      conf.setProperty(OozieClient.COORDINATOR_APP_PATH, s"$coordPath");
      // submit and start the workflow job
      oozieClient.submit(conf)
    }
  }

  /**
   * Helper function which writes a workflow/coordinator data and opens up permissions
   */
  private def writeScheduleData(path: String, content: Array[Byte]): Future[String] = {
    Future {
      val p = new Path(path)
      if (hadoopFS.exists(p)) {
        hadoopFS.delete(p, true)
      }
      val os = hadoopFS.create(p, true)
      os.write(content)
      os.flush()
      os.close()

      hadoopFS.setPermission(new Path(path), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))

      path
    }
  }

  /**
   * Create a new workflow
   *
   * @return Workflow path
   */
  def createWorkflow(dataset: Dataset): Future[String] = {
    val wfPath = s"$namenode$oozieScheduleHDFSPath/menas-oozie-schedule-wf-${dataset.name}-${dataset.version + 1}/workflow.xml"
    val content = getWorkflowFromTemplate(dataset)
    this.writeScheduleData(wfPath, content)
  }

  /**
   * Create a new coordinator
   *
   * @return Coordinator path
   */
  def createCoordinator(dataset: Dataset, wfPath: String): Future[String] = {
    val coordPath = s"$namenode$oozieScheduleHDFSPath/menas-oozie-schedule-coord-${dataset.name}-${dataset.version + 1}/coordinator.xml"
    val content = getCoordinatorFromTemplate(dataset, wfPath)
    this.writeScheduleData(coordPath, content)
  }
}
