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

package za.co.absa.enceladus.conformance.config

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.atum.persistence.S3Location
import za.co.absa.enceladus.common.config.PathConfig
import PathConfig.StringS3LocationExt


class PathConfigSuite extends FlatSpec with Matchers {

  "PathConfig" should "parse S3 path from String" in {
    Seq(
      // (path, expected parsed value)
      ("s3://mybucket-123/path/to/file.ext", S3Location("mybucket-123", "path/to/file.ext")),
      ("s3n://mybucket-123/path/to/ends/with/slash/", S3Location("mybucket-123", "path/to/ends/with/slash/")),
      ("s3a://mybucket-123.asdf.cz/path-to-$_file!@#$.ext", S3Location("mybucket-123.asdf.cz", "path-to-$_file!@#$.ext"))
    ).foreach { case (path, expectedLocation) =>
      path.toS3Location() shouldBe expectedLocation
    }
  }

  it should "fail parsing invalid S3 path from String" in {
    Seq(
      "s3x://mybucket-123/path/to/file/on/invalid/prefix",
      "s3://bb/some/path/but/bucketname/too/short"
    ).foreach { path =>
      assertThrows[IllegalArgumentException] {
        path.toS3Location()
      }
    }
  }

}
