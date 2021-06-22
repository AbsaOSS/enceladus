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

import java.io.File
import org.apache.hadoop.fs.Path

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FSDataInputStream, LocalFileSystem}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.SparkSession
import sun.security.krb5.internal.ktab.KeyTab
import za.co.absa.enceladus.utils.fs.{FileSystemUtils, HadoopFsUtils}

import scala.io.Source

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

    val confString = fs match {
      case _: DistributedFileSystem | _: LocalFileSystem =>
        HadoopFsUtils.getOrCreate(fs).getLocalOrDistributedFileContent(path)
      case _ => {
        val fileStream: FSDataInputStream = fs.open(new Path(path))
        Source.fromInputStream(fileStream).mkString
      }
    }
    val conf = ConfigFactory.parseString(confString)

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
    val fs = FileSystemUtils.getFileSystemFromPath(path)(spark.sparkContext.hadoopConfiguration)

    val localKeyTabPath = fs match {
      case _: DistributedFileSystem | _: LocalFileSystem => {
        HadoopFsUtils.getOrCreate(fs).getLocalPathToFileOrCopyToLocal(path)
      }
      case _ => {
        val localFile = File.createTempFile("enceladusFSUtils", "s3FileToLocalTemp")
        fs.copyToLocalFile(new Path(path), new Path(localFile.getAbsolutePath))

        localFile.getAbsolutePath
      }
    }

    val keytab = KeyTab.getInstance(localKeyTabPath)
    val username = keytab.getOneName.getName

    MenasKerberosCredentials(username, localKeyTabPath)
  }
}
