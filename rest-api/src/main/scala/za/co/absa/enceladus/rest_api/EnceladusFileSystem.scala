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

case class EnceladusFileSystem(fileSystem: Option[FileSystem]) {
  private val exception = new Exception("No FileSystem initialized")

  def getStatus: FsStatus = fileSystem.getOrElse(throw exception).getStatus

  def listStatus(path: Path): Array[FileStatus] = fileSystem.getOrElse(throw exception).listStatus(path)

  def isDirectory(path: Path): Boolean = fileSystem.getOrElse(throw exception).isDirectory(path)

  def exists(path: Path): Boolean = fileSystem.getOrElse(throw exception).exists(path)
}

object EnceladusFileSystem {
  def apply(fs: FileSystem): EnceladusFileSystem = EnceladusFileSystem(Some(fs))
  def apply(): EnceladusFileSystem = EnceladusFileSystem(None)
}
