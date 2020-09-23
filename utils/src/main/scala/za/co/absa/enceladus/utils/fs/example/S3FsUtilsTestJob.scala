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

package za.co.absa.enceladus.utils.fs.example

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region
import za.co.absa.atum.persistence.S3KmsSettings
import za.co.absa.atum.utils.S3Utils
import za.co.absa.enceladus.utils.fs.S3FsUtils

// todo remove or create a integtest like this instead.
// implementation is directly suited to be runnable locally with a saml profile.
object S3FsUtilsTestJob {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val bucketName = "putYourBucketBucketNameHere"

  def main(args: Array[String]): Unit = {
    val basePath = s"s3://$bucketName/ao-hdfs-data/hdfs_data/std/std_nf_dn"

    val sparkBuilder = SparkSession.builder().appName("Sample S3 Measurements 1 Job")
    val spark = sparkBuilder
      // .master("local")
      .getOrCreate()

    // This sample example relies on local credentials profile named "saml" with access to the s3 location defined below
    implicit val samlCredentialsProvider = S3Utils.getLocalProfileCredentialsProvider("saml")
    val kmsKeyId = System.getenv("TOOLING_KMS_KEY_ID") // load from an environment property in order not to disclose it here
    log.info(s"kmsKeyId from env loaded = ${kmsKeyId.take(10)}...")

    val s3utils = new S3FsUtils(Region.EU_WEST_1, S3KmsSettings(kmsKeyId)) {
      override val maxKeys = 5 // to test recursive listing/action
    }

    log.info(s"dir size of $basePath is:" + s3utils.getDirectorySize(basePath))
    log.info(s"dir size (no hidden) of $basePath is:" + s3utils.getDirectorySizeNoHidden(basePath))

    log.info(s"should exist:" + s3utils.exists(s"$basePath/1/2019/11/27/1/_INFO"))
    log.info(s"should not exist:" + s3utils.exists(s"$basePath/1/2019/11/27/1/_INFOxxx"))

    log.info("found version (1): "
      + s3utils.getLatestVersion(s"s3://$bucketName/superhero/publish", "2020-08-06"))

    log.info("found no version (0): "
      + s3utils.getLatestVersion(s"s3://$bucketName/aaa", "2020-08-06"))

    log.info(s"reading file content:" + s3utils.read(s"$basePath/1/2019/11/27/1/_INFO").take(50))

    log.info(s"should find no gz-s:" + s3utils.isNonSplittable(s"s3://$bucketName/gz-list/nogz"))
    log.info(s"should find some gz-s (and breakOut):" +
      s3utils.isNonSplittable(s"s3://$bucketName/gz-list/somegz"))

    val deletePath = s"s3://$bucketName/delete"
    log.info(s"deleting $deletePath: " + s3utils.deleteDirectoryRecursively(deletePath))
  }

}
