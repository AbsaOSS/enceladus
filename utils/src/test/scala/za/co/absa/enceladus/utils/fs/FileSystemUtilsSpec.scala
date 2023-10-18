/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.utils.fs

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.{AnyFunSuite, AnyFunSuiteLike}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class FileSystemUtilsSpec extends AnyFunSuiteLike with SparkTestBase {
  implicit val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration

  test("hdfs protocol default") {
    val fs = FileSystemUtils.getFileSystemFromPath("hdfs://my/path")
    assert(fs.getUri.toString == "hdfs://")
  }

  test("s3 protocol recognition and bucket set") {
    val fs = FileSystemUtils.getFileSystemFromPath("s3://my-bucket/my/path")
    assert(fs.getUri.toString == "s3://my-bucket")
  }

  test("s3a protocol recognition and bucket set") {
    val fs = FileSystemUtils.getFileSystemFromPath("s3a://my-bucket/my/path")
    assert(fs.getUri.toString == "s3a://my-bucket")
  }

}
