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

package za.co.absa.enceladus.rest_api.services


import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.mockito.Mockito
import za.co.absa.enceladus.model.menas.HDFSFolder

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class HDFSServiceSuite extends BaseServiceTest {

  //mocks
  private val fs = mock[FileSystem]

  //service
  private val hdfsService = new HDFSService(fs)

  //common test setup
  private val dirPathStr = "/tmp/dir"
  private val filePathStr = s"$dirPathStr/file"
  private val subdirPathStr = s"$dirPathStr/subdir"
  private val dirPath = new Path(dirPathStr)
  private val filePath = new Path(filePathStr)
  private val subdirPath = new Path(subdirPathStr)

  before {
    Mockito.reset(fs)

    val directoryFileStatus = mock[FileStatus]
    Mockito.when(directoryFileStatus.isDirectory).thenReturn(true)

    val nonDirectoryFileStatus = mock[FileStatus]
    Mockito.when(nonDirectoryFileStatus.isDirectory).thenReturn(false)

    // [[za.co.absa.enceladus.utils.fs.FileSystemUtils.FileSystemExt.isDirectoryWithDefault]] will be called on the fs
    Mockito.when(fs.getFileStatus(dirPath)).thenReturn(directoryFileStatus)
    Mockito.when(fs.getFileStatus(filePath)).thenReturn(nonDirectoryFileStatus)
    Mockito.when(fs.getFileStatus(subdirPath)).thenReturn(directoryFileStatus)
  }

  test("Calling HDFSService::exists should call fs::exists non-blockingly") {
    Await.result(hdfsService.exists(filePath), Duration(100, TimeUnit.MILLISECONDS))

    Mockito.verify(fs, Mockito.times(1)).exists(filePath)
  }

  test("Calling HDFSService::getFolder on file should return HDFSFolder without children non-blockingly") {
    val result = Await.result(hdfsService.getFolder(filePath), Duration(100, TimeUnit.MILLISECONDS))

    assert(result == HDFSFolder(filePathStr, "file", None))
  }

  test("Calling HDFSService::getFolder on dir with files should return HDFSFolder with children non-blockingly") {
    val fileStatus = new FileStatus()
    fileStatus.setPath(filePath)
    val subdirStatus = new FileStatus()
    subdirStatus.setPath(subdirPath)
    val listStatus = Array(fileStatus, subdirStatus)

    Mockito.when(fs.listStatus(dirPath)).thenReturn(listStatus)

    val result = Await.result(hdfsService.getFolder(dirPath), Duration(100, TimeUnit.MILLISECONDS))

    val empty = HDFSFolder("", "", None)
    val file = HDFSFolder(filePathStr, "file", None)
    val subdir = HDFSFolder(subdirPathStr, "subdir", Some(Array(empty)))
    val expected = HDFSFolder(dirPathStr, "dir", Some(Array(file, subdir)))
    assert(result == expected)
  }

}
