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

package za.co.absa.enceladus.rest.controllers

import java.util.concurrent.CompletableFuture

import org.apache.hadoop.fs.Path
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.api.HDFSFolder
import za.co.absa.enceladus.rest.services.HdfsService

import scala.concurrent._
import org.slf4j.LoggerFactory

@RestController
@RequestMapping(Array("/api/hdfs"))
class HDFSController @Autowired() (hdfsService: HdfsService) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import za.co.absa.enceladus.rest.utils.implicits._

  private val fs = hdfsService.fs

  @PostMapping(path = Array("/list"))
  def getHDFSFolder(@RequestBody path: String): CompletableFuture[HDFSFolder] = {
    val p = new Path(path)
    val res = if (!fs.isDirectory(p)) Future { HDFSFolder(p.toUri().getPath, p.getName, None) }
    else {
      Future {
        val status = fs.listStatus(p)
        val children = if (status.isEmpty) None else {
          Some(
            status.map({ x =>
              val child = x.getPath

              HDFSFolder(child.toUri().getPath, child.getName,
                if (fs.listStatus(child).isEmpty) None else Some(Seq(HDFSFolder("", "", None))))

            }).toSeq)
        }
        HDFSFolder(path, p.getName, children)
      }
    }

    res
  }

}