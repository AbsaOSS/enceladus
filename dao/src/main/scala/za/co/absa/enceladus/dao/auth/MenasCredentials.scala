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

package za.co.absa.enceladus.dao.auth

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.LocalFileSystem
import za.co.absa.commons.s3.SimpleS3Location.SimpleS3LocationExt
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.SparkSession
import sun.security.krb5.internal.ktab.KeyTab
import za.co.absa.commons.s3.SimpleS3Location
import za.co.absa.enceladus.utils.fs.FileSystemUtils.log
import za.co.absa.enceladus.utils.fs.{FileSystemUtils, HadoopFsUtils}

sealed abstract class MenasCredentials {
  val username: String
}

case class MenasPlainCredentials(username: String, password: String) extends MenasCredentials

case class MenasKerberosCredentials(username: String, keytabLocation: String) extends MenasCredentials

case object InvalidMenasCredentials extends MenasCredentials {
  override val username: String = "invalid-credentials"
}

object MenasPlainCredentials {
  /**
    * Instantiates [[MenasCredentials]] from a credentials file located either on the local file system or on HDFS.
    *
    * @param path A path to a Menas Credentials file.
    * @return An instance of Menas Credentials.
    */
  def fromFile(path: String)(implicit spark: SparkSession): MenasPlainCredentials = {
    val fs =  FileSystemUtils.getFileSystemFromPath(path)(spark.sparkContext.hadoopConfiguration)

    val conf = fs match {
      case _: LocalFileSystem => {
        log.info("Local FS")
        val str = HadoopFsUtils.getOrCreate(fs).getLocalOrDistributedFileContent(path)
        log.info(str)
        ConfigFactory.parseString(str)
      }
      case _: DistributedFileSystem => {
        log.debug("HDFS FS")
        val str = HadoopFsUtils.getOrCreate(fs).getLocalOrDistributedFileContent(path)
        log.info(str)
        ConfigFactory.parseString(str)
      }
      case _ => {
        log.info("S3 FS")
        val s3Path: SimpleS3Location = path.toSimpleS3Location.get

        val client = new AmazonS3Client()
        val s3Object = client.getObject(s3Path.bucketName, s3Path.path)
        val content: S3ObjectInputStream = s3Object.getObjectContent
        val str1 = scala.io.Source.fromInputStream(content).mkString
        log.info(str1)

        ConfigFactory.parseString(str1)
      }
    }

    MenasPlainCredentials(conf.getString("username"), conf.getString("password"))
  }
}

object MenasKerberosCredentials {
  /**
    * Instantiates [[MenasCredentials]] from file either on the local file system or on HDFS.
    *
    * @param path A path to a Kerberos keytab file.
    * @return An instance of Menas Credentials.
    */
  def fromFile(path: String)(implicit spark: SparkSession): MenasKerberosCredentials = {
    val fs =  FileSystemUtils.getFileSystemFromPath(path)(spark.sparkContext.hadoopConfiguration)

    val localKeyTabPath = HadoopFsUtils.getOrCreate(fs).getLocalPathToFileOrCopyToLocal(path)
    val keytab = KeyTab.getInstance(localKeyTabPath)
    val username = keytab.getOneName.getName

    MenasKerberosCredentials(username, localKeyTabPath)
  }
}
