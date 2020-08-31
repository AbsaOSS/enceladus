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

package za.co.absa.enceladus.common.config

import software.amazon.awssdk.regions.Region
import za.co.absa.atum.persistence.S3Location

/**
 *
 * @param rawPath Input path of the job
 * @param publishPath Output path of the job
 * @param standardizationPath In case of StandardizationJob and ConformanceJob it should be None and for
 *                            StandardizationConformanceJob it should represent the intermediate standardization path
 */
case class PathConfig(rawPath: String, publishPath: String, standardizationPath: String)

object PathConfig {

  // hint: https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules
  val S3LocationRx = "s3(?:a|n)?://([-a-z0-9.]{3,63})/(.*)".r

   implicit class StringS3LocationExt(path: String) {

     // TODO figure out where to get region from
      def toS3Location(region: Region = Region.EU_WEST_1): S3Location = {
          path match {
            case S3LocationRx(bucketName, path) => S3Location(bucketName, path, region)
            case _ => throw new IllegalArgumentException(s"Could not parse S3 Location from $path using rx $S3LocationRx.")
          }
      }
   }

}
