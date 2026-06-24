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

package za.co.absa.enceladus.utils.fs

import org.scalatest.funsuite.AnyFunSuite

class FileSystemUtilsSuite extends AnyFunSuite {

  test("s3BucketUri preserves S3-compatible path schemes") {
    assert(FileSystemUtils.s3BucketUri("s3://bucket/path/to/file", "bucket").toString == "s3://bucket")
    assert(FileSystemUtils.s3BucketUri("s3a://bucket/path/to/file", "bucket").toString == "s3a://bucket")
    assert(FileSystemUtils.s3BucketUri("s3n://bucket/path/to/file", "bucket").toString == "s3n://bucket")
  }
}
