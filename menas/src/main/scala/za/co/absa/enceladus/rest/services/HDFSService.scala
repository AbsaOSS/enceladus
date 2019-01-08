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

package za.co.absa.enceladus.rest.services

import org.apache.hadoop.fs.{FileSystem, Path}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import za.co.absa.enceladus.model.api.HDFSFolder

import scala.concurrent.Future

@Component
class HDFSService @Autowired() (fs: FileSystem) {
  import scala.concurrent.ExecutionContext.Implicits.global

  def exists(path: Path): Future[Boolean] = Future {
    fs.exists(path)
  }

  def getFolder(path: Path): Future[HDFSFolder] = Future {
    if (!fs.isDirectory(path)) {
      HDFSFolder(path.toUri.getPath, path.getName, None)
    } else {
      val status = fs.listStatus(path)
      val children = if (status.isEmpty) None else {
        Some(
          status.map({ x =>
            val child = x.getPath
            HDFSFolder(child.toUri.getPath, child.getName,
              if (fs.listStatus(child).isEmpty) None else Some(Seq(HDFSFolder("", "", None))))

          }).toSeq)
      }
      HDFSFolder(path.toUri.getPath, path.getName, children)
    }
  }

}
