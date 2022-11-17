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

package za.co.absa.enceladus.rest_api

import org.apache.hadoop.fs.{FileStatus, FileSystem, FsStatus, Path}

sealed trait EnceladusFileSystem {
  def getStatus: FsStatus
  def listStatus(path: Path): Array[FileStatus]
  def isDirectory(path: Path): Boolean
  def exists(path: Path): Boolean
}
object EnceladusFileSystem {
  def apply(fs: FileSystem): EnceladusFileSystem = HdfsFileSystem(fs)
  def apply() : EnceladusFileSystem = empty
  val empty: EnceladusFileSystem = NoFileSystem

  private case class HdfsFileSystem(fileSystem: FileSystem) extends EnceladusFileSystem {
    def getStatus: FsStatus = fileSystem.getStatus
    def listStatus(path: Path): Array[FileStatus] = fileSystem.listStatus(path)
    def isDirectory(path: Path): Boolean = fileSystem.isDirectory(path)
    def exists(path: Path): Boolean = fileSystem.exists(path)
  }
  private case object NoFileSystem extends EnceladusFileSystem {
    val exception = new Exception("No FileSystem initialized")
    def getStatus: FsStatus = throw exception
    def listStatus(path: Path): Array[FileStatus] = throw exception
    def isDirectory(path: Path): Boolean = throw exception
    def exists(path: Path): Boolean = throw exception
  }

}
