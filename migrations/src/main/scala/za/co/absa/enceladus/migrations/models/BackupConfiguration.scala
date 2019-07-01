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

package za.co.absa.enceladus.migrations.models

import java.io.File

import com.mongodb.ConnectionString
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

/**
 * BackupConfiguration is the configurations used for creating or restoring a database dump.
 * By default the local file system is used, but you can also provide an optional file system
 * configuration argument in case you want to use a different file system (e.g. HDFS, FTP file system, etc.)
 *
 * @param dumpFilepath     The path on the file system to store a database dump or restore from, including filename
 *                         The database dump will be gzipped and archived (e.g. "/path/to/backups/db.archive")
 * @param connectionString The MongoDB connection string
 * @param database         The MongoDB database to create a dump or restore
 * @param fsConf           An optional file system configuration
 */
final case class BackupConfiguration(dumpFilepath: String, connectionString: String, database: String, fsConf: Option[Configuration] = None) {

  /**
   * Returns a list of strings containing the command line arguments for mongodump/mongorestore
   * extracted from the given connection string
   *
   * @return List of command line arguments
   */
  private[migrations] def getCmdArgs: List[String] = {
    val connStr = new ConnectionString(connectionString)
    val host = Option(connStr.getRequiredReplicaSetName) match {
      case Some(replicaSetName) => Option(s"--host" :: Option(connStr.getHosts).map(_.asScala.mkString(",")).map(hosts => s"$replicaSetName/$hosts").get :: Nil)
      case None                 => Option(connStr.getHosts).map(hosts => s"--host" :: hosts.asScala.mkString(",") :: Nil)
    }

    val username = Option(connStr.getUsername).map(username => s"--username" :: username :: Nil)
    val password = Option(connStr.getPassword).map(passwd => s"--password" :: passwd.mkString("") :: Nil)
    val db = Option(s"--db" :: database :: Nil)
    val authenticationDatabase = Option(connStr.getCredential).map(_.getSource).map(source => s"--authenticationDatabase" :: source :: Nil)
    val readPreference = Option(connStr.getReadPreference).map(preference => s"--readPreference" :: preference.getName :: Nil)
    val ssl = Option(connStr.getSslEnabled).filter(_.booleanValue()).map(_ => List("--ssl"))
    val sslCaFile = "ssl[cC][aA]File=(.+?)(?:&|$)".r.findFirstMatchIn(connectionString).map(captured => s"--sslCAFile" :: captured.group(1) :: Nil)
    val gzip = Some(List("--gzip"))
    List(host, username, password, db, authenticationDatabase, readPreference, ssl, sslCaFile, gzip).flatten.flatten
  }

  /**
   * Returns a path to a temporary database dump file on the local file system
   *
   * @return Path to a temporary database dump file
   */
  private[migrations] def getTempFilepath: String = {
    val tempDir = System.getProperty("java.io.tmpdir")
    val fileName = getFileName
    s"$tempDir${File.separator}$fileName"
  }

  private[migrations] def getFileName: String = {
    dumpFilepath.split(File.separatorChar).last
  }

  private[migrations] def getFilePath: String = {
    dumpFilepath.split(File.separatorChar).dropRight(1).mkString(File.separator)
  }

}
