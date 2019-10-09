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

package za.co.absa.enceladus

import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

/**
  * Unit tests for File system utils
  */
class FsUtilsSpec extends FlatSpec with Matchers with SparkTestBase {
  val fsUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)

  "splitUriPath" should "split URI and path" in
  {
    val path = new Path("hdfs://some-host:8020/user/data/input")
    val (prefix, rawPath) = fsUtils.splitUriPath(path)
    prefix shouldEqual "hdfs://some-host:8020"
    rawPath shouldEqual "/user/data/input"
  }

  "splitUriPath" should "split not split a path without URI prefix" in
    {
      val path = new Path("/projects/coreconformance/publish/dataset")
      val (prefix, rawPath) = fsUtils.splitUriPath(path)
      prefix shouldEqual ""
      rawPath shouldEqual "/projects/coreconformance/publish/dataset"
    }

  "splitUriPath" should "split not split relative path" in
    {
      val path = new Path("data/input")
      val (prefix, rawPath) = fsUtils.splitUriPath(path)
      prefix shouldEqual ""
      rawPath shouldEqual "data/input"
    }

  "getDirectorySize" should "return the size of all files in a directory" in {
    val dirSize = fsUtils.getDirectorySize("src/test/resources/test_data/test_dir")
    assert(dirSize == 47L)
  }

  "getDirectorySize" should "return the size of all files recursively" in {
    val dirSize = fsUtils.getDirectorySize("src/test/resources/test_data/test_dir2")
    assert(dirSize == 87L)
  }

  "getDirectorySizeNoHidden" should "return the size of all non-hidden files in a directory" in {
    val dirSize = fsUtils.getDirectorySizeNoHidden("src/test/resources/test_data/test_dir")
    assert(dirSize == 20L)
  }

  "getDirectorySizeNoHidden" should "return the size of all non-hidden files recursively along non-hidden paths" in {
    val dirSize = fsUtils.getDirectorySizeNoHidden("src/test/resources/test_data/test_dir2")
    assert(dirSize == 40L)
  }

}
