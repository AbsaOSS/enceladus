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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class HdfsService {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val hadoopConfDir = sys.env.getOrElse("HADOOP_CONF_DIR", throw new IllegalStateException("Missing HADOOP_CONF_DIR environment variable."))

  val hadoopConf = new Configuration
  hadoopConf.addResource(new Path(hadoopConfDir, "core-site.xml"))
  hadoopConf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"))
  logger.info(s"Using hadoop configuration from $hadoopConfDir")

  val fs: FileSystem = FileSystem.get(hadoopConf)

}
