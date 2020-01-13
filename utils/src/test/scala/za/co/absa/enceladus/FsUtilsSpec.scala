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

package za.co.absa.enceladus

import java.io.FileNotFoundException

import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

/**
  * Unit tests for File system utils
  */
class FsUtilsSpec extends WordSpec with Matchers with SparkTestBase {
  val fsUtils = new FileSystemVersionUtils(spark.sparkContext.hadoopConfiguration)

  "splitUriPath" should {
    "split URI and path" in {
      val path = new Path("hdfs://some-host:8020/user/data/input")
      val (prefix, rawPath) = fsUtils.splitUriPath(path)
      prefix shouldEqual "hdfs://some-host:8020"
      rawPath shouldEqual "/user/data/input"
    }

    "not split a path without URI prefix" in {
      val path = new Path("/projects/coreconformance/publish/dataset")
      val (prefix, rawPath) = fsUtils.splitUriPath(path)
      prefix shouldEqual ""
      rawPath shouldEqual "/projects/coreconformance/publish/dataset"
    }

    "not split relative path" in {
      val path = new Path("data/input")
      val (prefix, rawPath) = fsUtils.splitUriPath(path)
      prefix shouldEqual ""
      rawPath shouldEqual "data/input"
    }
  }

  "getDirectorySize" should {
    "throw an exception if the specified path does not exist" in {
      intercept[FileNotFoundException] {
        fsUtils.getDirectorySize("src/test/resources/test_data/not_exist")
      }
    }

    "return the file size if a single file is specified" in {
      val dirSize = fsUtils.getDirectorySize("src/test/resources/test_data/test_dir/dummy.txt")
      assert(dirSize == 20L)
    }

    "return the file size if a single hidden file is specified" in {
      val dirSize = fsUtils.getDirectorySize("src/test/resources/test_data/test_dir/_hidden_dummy.txt")
      assert(dirSize == 27L)
    }

    "return the size of all files in a directory" in {
      val dirSize = fsUtils.getDirectorySize("src/test/resources/test_data/test_dir")
      assert(dirSize == 47L)
    }

    "return the size of all files recursively" in {
      val dirSize = fsUtils.getDirectorySize("src/test/resources/test_data/test_dir2")
      assert(dirSize == 87L)
    }
  }

  "getDirectorySizeNoHidden" should {
    "throw an exception if the specified path does not exist" in {
      intercept[FileNotFoundException] {
        fsUtils.getDirectorySizeNoHidden("src/test/resources/test_data/not_exist")
      }
    }

    "return the file size if a single file is specified" in {
      val dirSize = fsUtils.getDirectorySizeNoHidden("src/test/resources/test_data/test_dir/dummy.txt")
      assert(dirSize == 20L)
    }

    "return the file size if a single hidden file is specified" in {
      val dirSize = fsUtils.getDirectorySizeNoHidden("src/test/resources/test_data/test_dir/_hidden_dummy.txt")
      assert(dirSize == 27L)
    }

    "return the size of all non-hidden files in a directory" in {
      val dirSize = fsUtils.getDirectorySizeNoHidden("src/test/resources/test_data/test_dir")
      assert(dirSize == 20L)
    }

    "return the size of all non-hidden files recursively along non-hidden paths" in {
      val dirSize = fsUtils.getDirectorySizeNoHidden("src/test/resources/test_data/test_dir2")
      assert(dirSize == 40L)
    }

    "return the size of all non-hidden files if a hidden directory is specified explicitly" in {
      val dirSize = fsUtils.getDirectorySizeNoHidden("src/test/resources/test_data/test_dir2/_inner_dir")
      assert(dirSize == 20L)
    }
  }

}
