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

package za.co.absa.enceladus.menas.repositories

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date
import java.util.HashMap
import java.util.{ Map => JavaMap }
import java.util.Properties
import java.util.TimeZone
import java.util.concurrent.Callable

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.oozie.client.OozieClient
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Repository

import OozieRepository.dateFormat
import javax.security.auth.kerberos.KerberosPrincipal
import javax.security.auth.kerberos.KeyTab
import sun.security.krb5.{ Config => Krb5Config }
import sun.security.krb5.KrbAsReqBuilder
import sun.security.krb5.PrincipalName
import sun.security.krb5.internal.KDCOptions
import sun.security.krb5.internal.ccache.CredentialsCache
import za.co.absa.enceladus.menas.exceptions.OozieActionException
import za.co.absa.enceladus.menas.exceptions.OozieConfigurationException
import za.co.absa.enceladus.menas.models.OozieCoordinatorStatus
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.menas.scheduler.RuntimeConfig
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

object OozieRepository {
  private lazy val dateFormat = {
    TimeZoneNormalizer.normalizeJVMTimeZone() //ensure time zone normalization before SimpleDateFormat creation
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
  }
}

@Repository
class OozieRepository @Autowired() (oozieClientRes: Either[OozieConfigurationException, OozieClient],
    datasetMongoRepository: DatasetMongoRepository,
    hadoopFS: FileSystem,
    hadoopConf: Configuration) extends InitializingBean {

  import scala.concurrent.ExecutionContext.Implicits.global

  @Value("${menas.oozie.schedule.hdfs.path:}")
  val oozieScheduleHDFSPath: String = ""

  @Value("${menas.oozie.timeZone:Africa/Ceuta}")
  val oozieTimezone: String = ""

  @Value("${menas.oozie.sharelibForSpark:spark}")
  val oozieShareLib: String = ""

  @Value("${menas.oozie.libpath:}")
  val oozieLibPath: String = ""

  @Value("${menas.oozie.enceladusJarLocation:}")
  val enceladusJarLocation: String = ""

  @Value("${menas.oozie.mavenStandardizationJarLocation:}")
  val standardizationJarPath: String = ""

  @Value("${menas.oozie.mavenConformanceJarLocation:}")
  val conformanceJarPath: String = ""

  @Value("${menas.oozie.mavenRepoLocation:}")
  val mavenRepoLocation: String = ""

  @Value("${menas.oozie.menasApiURL:}")
  val menasApiURL: String = ""

  @Value("${menas.oozie.splineMongoURL:}")
  val splineMongoURL: String = ""

  @Value("${menas.oozie.sparkConf.surroundingQuoteChar:}")
  val sparkConfQuotes: String = ""

  @Value("${menas.oozie.proxyUser:}")
  val oozieProxyUser: String = ""

  @Value("${menas.oozie.proxyUserKeytab:}")
  val oozieProxyUserKeytab: String = ""

  @Value("${menas.auth.kerberos.krb5conf:}")
  val krb5conf: String = ""

  @Value("#{${menas.oozie.extraSparkConfigs:{'spark.ui.enabled': 'true'}}}")
  val sparkExtraConfigs: JavaMap[String, String] = new HashMap[String, String]()

  private val classLoader = Thread.currentThread().getContextClassLoader
  private val workflowTemplate = getTemplateFile("scheduling/oozie/workflow_template.xml")
  private val coordinatorTemplate = getTemplateFile("scheduling/oozie/coordinator_template.xml")
  private val namenode = hadoopConf.get("fs.defaultFS")
  private val resourceManager = hadoopConf.get("yarn.resourcemanager.address")
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def afterPropertiesSet() {
    logger.info(s"Enceladus Jar Location: $enceladusJarLocation")
    logger.info(s"Stanrdardization Jar Path: $standardizationJarPath")
    logger.info(s"Conformance Jar Path: $conformanceJarPath")
    //ensure that relevant jars are properly loaded in HDFS, otherwise initialize
    this.initializeJars()
  }

  private def validateProperties(logWarnings: Boolean = false): Boolean = {
    Seq((oozieScheduleHDFSPath, "menas.oozie.schedule.hdfs.path"),
      (enceladusJarLocation, "menas.oozie.enceladusJarLocation"),
      (standardizationJarPath, "menas.oozie.mavenStandardizationJarLocation"),
      (conformanceJarPath, "menas.oozie.mavenConformanceJarLocation"),
      (mavenRepoLocation, "menas.oozie.mavenRepoLocation"),
      (menasApiURL, "menas.oozie.menasApiURL"),
      (splineMongoURL, "menas.oozie.splineMongoURL")).map(p => validateProperty(p._1, p._2, logWarnings)).reduce(_ && _)
  }

  private def validateProperty(prop: String, propName: String, logWarnings: Boolean = false): Boolean = {
    if (prop == null || prop.isEmpty) {
      if (logWarnings) {
        logger.warn(s"Oozie support disabled. Missing required configuration property $propName")
      }
      false
    } else {
      true
    }
  }

  private def initializeJars() {
    if (this.isOozieEnabled(true)) {
      val hdfsStdPath = new Path(s"$enceladusJarLocation$standardizationJarPath")
      val hdfsConfPath = new Path(s"$enceladusJarLocation$conformanceJarPath")
      val mavenStdPath = s"$mavenRepoLocation$standardizationJarPath"
      val mavenConfPath = s"$mavenRepoLocation$conformanceJarPath"

      val resFutureStd = this.downloadFile(mavenStdPath, hdfsStdPath)
      resFutureStd.onSuccess {
        case _ => logger.info(s"Standardization jar loaded to $hdfsStdPath")
      }
      resFutureStd.onFailure {
        case err: Throwable =>
          hadoopFS.delete(hdfsStdPath, true)
      }

      val resFutureConf = this.downloadFile(mavenConfPath, hdfsConfPath)
      resFutureConf.onSuccess {
        case _ => logger.info(s"Conformance jar loaded to $hdfsConfPath")
      }
      resFutureConf.onFailure {
        case err: Throwable =>
          hadoopFS.delete(hdfsConfPath, true)
      }

    }
  }

  /**
   * Used for downloading the jar either from maven or local repo
   */
  private def downloadFile(url: String, hadoopPath: Path) = {
    Future {
      if (!hadoopFS.exists(hadoopPath) || hadoopFS.getStatus(hadoopPath).getCapacity == 0) {
        logger.info(s"Uploading jar from $url to $hadoopPath")
        val connection = new URL(url).openConnection()
        connection match {
          case httpConn: HttpURLConnection => httpConn.setRequestMethod("GET")
          case _                           => Unit
        }

        val in = connection.getInputStream
        val os = hadoopFS.create(hadoopPath, true)

        try {
          IOUtils.copy(in, os)
        } finally {
          os.flush()
          os.close()
          in.close()
        }

      }
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
   * This is a helper function for impersonating oozie calls using the proper proxy user if configured
   *
   * @param user User to impersonate
   * @fn Oozie action to perform - Important to note that this should be Oozie action only (only wrap the call to oozieclient)
   */
  private def impersonateWrapper[T](user: String)(fn: () => T) = {
    if (oozieProxyUser.isEmpty || oozieProxyUserKeytab.isEmpty) {
      logger.info("Oozie impersonation disabled. missing required configuration parameters 'za.co.absa.enceladus.menas.oozie.proxyUser'" +
          " and/or 'za.co.absa.enceladus.menas.oozie.proxyUserKeytab'")
      fn()
    } else {
      //first we login as the proxy user
      logger.info(s"impersonateWrapper Going to log in as $oozieProxyUser")

      val principal = new PrincipalName(oozieProxyUser, PrincipalName.KRB_NT_PRINCIPAL)

      logger.info(s"impersonateWrapper Creating credentials cache.")
      val cache = CredentialsCache.create(principal)

      logger.info(s"impersonateWrapper Reading keytab file ${oozieProxyUserKeytab}")
      val kt = KeyTab.getInstance(new KerberosPrincipal(oozieProxyUser), new java.io.File(oozieProxyUserKeytab))
      val builder = new KrbAsReqBuilder(principal, kt);
      val opt = new KDCOptions();
      opt.set(KDCOptions.RENEWABLE, true)
      builder.setOptions(opt)

      val realm = Krb5Config.getInstance.getDefaultRealm
      logger.info(s"impersonateWrapper Using realm ${realm}")

      val serviceName = PrincipalName.tgsService(realm, realm)
      logger.info(s"impersonateWrapper Target ${serviceName}")
      builder.setTarget(serviceName)

      logger.info(s"impersonateWrapper Logging in")
      builder.action()

      val creds = builder.getCCreds
      builder.destroy()

      logger.info(s"impersonateWrapper Updating ticket cache ${cache.toString()}")
      cache.update(creds)
      cache.save()

      OozieClient.doAs(user, new Callable[T] {
        override def call(): T = {
          logger.info(s"impersonateWrapper using Oozie impersonation, doAs(${user})")
          //call the user-specified function
          fn()
        }
      })
    }
  }

  /**
   * Whether or not oozie is enabled/configured
   */
  def isOozieEnabled(logWarnings: Boolean = false): Boolean = {
    this.validateProperties(logWarnings) && oozieClientRes.isRight
  }

  private def getOozieClient[T](fn: OozieClient => Future[T]): Future[T] = {
    oozieClientRes match {
      case Right(client) => fn(client)
      case Left(ex)      => Future.failed(ex)
    }
  }

  private def getOozieClientWrap[T](fn: OozieClient => T): Future[T] = {
    getOozieClient({ cl: OozieClient =>
      Future(fn(cl))
    })
  }

  /**
   * Get status of submitted coordinator
   */
  def getCoordinatorStatus(coordId: String, runtimeParams: RuntimeConfig): Future[OozieCoordinatorStatus] = {
    getOozieClientWrap({ oozieClient =>
      val jobInfo = impersonateWrapper(runtimeParams.sysUser) { () =>
        oozieClient.getCoordJobInfo(coordId)
      }
      val nextMaterializeTime = if (jobInfo.getNextMaterializedTime == null) {
        ""
      } else {
        dateFormat.format(jobInfo.getNextMaterializedTime)
      }
      OozieCoordinatorStatus(jobInfo.getStatus, nextMaterializeTime)
    })
  }

  /**
   * Kill a running coordinator
   */
  def killCoordinator(coordId: String, runtimeParams: RuntimeConfig): Future[Unit] = {
    getOozieClientWrap({ oozieClient =>
      impersonateWrapper(runtimeParams.sysUser) { () =>
        oozieClient.kill(coordId)
      }
    })
  }

  /**
   * Get workflow from teplate - this fills in all variables and returns representation of the workflow
   */
  private def getWorkflowFromTemplate(ds: Dataset): Array[Byte] = {
    //Here libpath takes precedence over sharelib
    val shareLibConfig = if(oozieLibPath.nonEmpty) "" else
      s"""
         |<parameters>
         |  <property>
         |    <name>oozie.action.sharelib.for.spark</name>
         |    <value>$oozieShareLib</value>
         |  </property>
         |</parameters>
      """.stripMargin
    import scala.collection.JavaConversions._
    val extraSparkConfString = sparkExtraConfigs.map({case (k, v) => s"--conf $sparkConfQuotes$k=$v$sparkConfQuotes"}).mkString("\n")
    val schedule = ds.schedule.get
    val runtimeParams = schedule.runtimeParams
    workflowTemplate.replaceAllLiterally("$stdAppName", s"Menas Schedule Standardization ${ds.name} (${ds.version})")
      .replaceAllLiterally("$confAppName", s"Menas Schedule Conformance ${ds.name} (${ds.version})")
      .replaceAllLiterally("$stdJarPath", s"$enceladusJarLocation$standardizationJarPath")
      .replaceAllLiterally("$confJarPath", s"$enceladusJarLocation$conformanceJarPath")
      .replaceAllLiterally("$datasetVersion", schedule.datasetVersion.toString)
      .replaceAllLiterally("$datasetName", ds.name)
      .replaceAllLiterally("$mappingTablePattern", schedule.mappingTablePattern.map(_.trim).filter(_.nonEmpty).getOrElse("reportDate={0}-{1}-{2}").trim)
      .replaceAllLiterally("$dataFormat", schedule.rawFormat.name)
      .replaceAllLiterally("$otherDFArguments", schedule.rawFormat.getArguments.map(arg => s"<arg>$arg</arg>").mkString("\n"))
      .replaceAllLiterally("$jobTracker", resourceManager)
      .replaceAllLiterally("$sharelibForSpark", shareLibConfig)
      .replaceAllLiterally("$nameNode", namenode)
      .replaceAllLiterally("$menasRestURI", menasApiURL)
      .replaceAllLiterally("$splineMongoURL", splineMongoURL)
      .replaceAllLiterally("$stdNumExecutors", runtimeParams.stdNumExecutors.toString)
      .replaceAllLiterally("$stdExecutorMemory", s"${runtimeParams.stdExecutorMemory}g")
      .replaceAllLiterally("$confNumExecutors", runtimeParams.confNumExecutors.toString)
      .replaceAllLiterally("$confExecutorMemory", s"${runtimeParams.confExecutorMemory}g")
      .replaceAllLiterally("$driverCores", s"${runtimeParams.driverCores}")
      .replaceAllLiterally("$menasKeytabFile", s"${getCredsOrKeytabArgument(runtimeParams.menasKeytabFile, namenode)}")
      .replaceAllLiterally("$sparkConfQuotes", sparkConfQuotes)
      .replaceAllLiterally("$extraSparkConfString", extraSparkConfString)
      .getBytes("UTF-8")
  }

  private def getCredsOrKeytabArgument(filename: String, protocol: String): String = {
    if (filename.toLowerCase.trim.endsWith(".properties")) {
      s"""<arg>--menas-credentials-file</arg>
         |<arg>$protocol$filename</arg>""".stripMargin
    } else {
      s"""<arg>--menas-auth-keytab</arg>
         |<arg>$protocol$filename</arg>""".stripMargin
    }
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
    coordinatorTemplate.replaceAllLiterally("$coordName", s"Menas Schedule Coordinator ${ds.name} (${ds.version})")
      .replaceAllLiterally("$cronTiming", schedule.scheduleTiming.getCronSchedule)
      .replaceAllLiterally("$reportDateOffset", schedule.reportDateOffset.toString)
      .replaceAllLiterally("$timezone", oozieTimezone)
      .replaceAllLiterally("$startDate", dateFormat.format(startDate))
      .replaceAllLiterally("$endDate", dateFormat.format(endDate))
      .replaceAllLiterally("$wfApplicationPath", wfPath).getBytes("UTF-8")
  }

  /**
   * Get oozie properties
   */
  private def getOozieConf(oozieClient: OozieClient, runtimeParams: RuntimeConfig): Properties = {
    val conf = oozieClient.createConfiguration()
    conf.setProperty("jobTracker", resourceManager)
    conf.setProperty("nameNode", namenode)
    if (oozieLibPath.isEmpty()) {
      conf.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "True")
    } else {
      conf.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "False")
      conf.setProperty(OozieClient.LIBPATH, oozieLibPath)
    }
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
    getOozieClientWrap { oozieClient =>
      val conf = getOozieConf(oozieClient, runtimeParams)
      conf.setProperty(OozieClient.COORDINATOR_APP_PATH, s"$coordPath")
      impersonateWrapper(runtimeParams.sysUser) { () =>
        oozieClient.submit(conf)
      }
    }
  }

  /**
   * Run a workflow now
   */
  def runWorkflow(wfPath: String, runtimeParams: RuntimeConfig, reportDate: String): Future[String] = {
    getOozieClient { oozieClient =>
      val conf = getOozieConf(oozieClient, runtimeParams)
      conf.setProperty(OozieClient.APP_PATH, wfPath)
      conf.setProperty("reportDate", reportDate)
      Try {
        impersonateWrapper(runtimeParams.sysUser) { () =>
          oozieClient.run(conf)
        }
      } match {
        case Success(x) => Future.successful(x)
        case Failure(e) => Future.failed(OozieActionException(e.getMessage, e.getCause))
      }
    }
  }

  /**
   * Suspend a coordinator
   */
  def suspend(coordId: String, runtimeParams: RuntimeConfig): Future[Unit] = {
    getOozieClientWrap { oozieClient =>
      impersonateWrapper(runtimeParams.sysUser) { () =>
        oozieClient.suspend(coordId)
      }
    }
  }

  /**
   * Resume a coordinator
   */
  def resume(coordId: String, runtimeParams: RuntimeConfig): Future[Unit] = {
    getOozieClientWrap { oozieClient =>
      impersonateWrapper(runtimeParams.sysUser) { () =>
        oozieClient.resume(coordId)
      }
    }
  }

  /**
   * Helper function which writes a workflow/coordinator data and opens up permissions
   */
  private def writeScheduleData(path: String, content: Array[Byte]): Future[String] = {
    Future {
      val p = new Path(path)
      if (hadoopFS.exists(p)) {
        logger.warn(s"Schedule $path already exists! Overwriting")
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
