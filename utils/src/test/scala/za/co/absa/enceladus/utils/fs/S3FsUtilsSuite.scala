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

import org.mockito.captor.{ArgCaptor, Captor}
import org.mockito.scalatest.IdiomaticMockito
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import za.co.absa.atum.persistence.S3KmsSettings

import scala.collection.JavaConverters._

class S3FsUtilsSuite extends AnyFlatSpec with IdiomaticMockito with Matchers {

  val kmsSettigns = S3KmsSettings("testingKeyId123")
  val region = Region.EU_WEST_2

  implicit val credentialsProvider = DefaultCredentialsProvider.create()

  // common fixture for all tests
  def fixture = new {
    val mockedS3Client = mock[S3Client]
    val mockedS3FsUtils = new S3FsUtils(region, kmsSettigns) {
      override def getS3Client: S3Client = mockedS3Client

      override val maxKeys = 3 // to test recursion for listing
    }
  }

  "S3FsUtilsTest" should "detect exiting file" in {
    val f = fixture
    val path = "s3://bucket1/path/to/existing.file"

    // mock S3 response for exist
    val mockedResponse: HeadObjectResponse = mock[HeadObjectResponse]
    Mockito.when(f.mockedS3Client.headObject(any[HeadObjectRequest])).thenReturn(mockedResponse)

    val existResult = f.mockedS3FsUtils.exists(path)

    // verify request content
    val requestCaptor: Captor[HeadObjectRequest] = ArgCaptor[HeadObjectRequest]
    Mockito.verify(f.mockedS3Client).headObject(requestCaptor.capture)
    val capturedGetRequest = requestCaptor.value

    capturedGetRequest.bucket shouldBe "bucket1"
    capturedGetRequest.key shouldBe "path/to/existing.file"

    // verify returned value
    existResult shouldBe true
  }

  it should "detect non-exiting file" in {
    val f = fixture
    val path = "s3://bucket1b/path/to/non-existing.file"

    // mock S3 response for exist
    Mockito.when(f.mockedS3Client.headObject(any[HeadObjectRequest]))
      .thenThrow(NoSuchKeyException.builder.message("the file does not exist!").build())

    val existResult = f.mockedS3FsUtils.exists(path)

    // verify request content
    val requestCaptor: Captor[HeadObjectRequest] = ArgCaptor[HeadObjectRequest]
    Mockito.verify(f.mockedS3Client).headObject(requestCaptor.capture)
    val capturedGetRequest = requestCaptor.value

    capturedGetRequest.bucket shouldBe "bucket1b"
    capturedGetRequest.key shouldBe "path/to/non-existing.file"

    // verify returned value
    existResult shouldBe false
  }

  it should "read data from S3 path" in {
    val f = fixture
    val path = "s3://bucket2/path/to/read.file"
    val mockedFileContent = "This is the file content on S3"

    val mockedResponseWithContent: ResponseBytes[GetObjectResponse] = mock[ResponseBytes[GetObjectResponse]]

    // mock S3 response
    Mockito.when(f.mockedS3Client.getObjectAsBytes(ArgumentMatchers.any[GetObjectRequest])).thenReturn(mockedResponseWithContent)
    Mockito.when(mockedResponseWithContent.asUtf8String()).thenReturn(mockedFileContent)

    val readingResult = f.mockedS3FsUtils.read(path)

    // verify request content
    val requestCaptor: Captor[GetObjectRequest] = ArgCaptor[GetObjectRequest]
    Mockito.verify(f.mockedS3Client).getObjectAsBytes(requestCaptor.capture)
    val capturedGetRequest = requestCaptor.value

    capturedGetRequest.bucket shouldBe "bucket2"
    capturedGetRequest.key shouldBe "path/to/read.file"

    // verify returned value
    readingResult shouldBe mockedFileContent
  }

  private case class MockedObjectDef(path: String, size: Long = 0L) {
    def toObject: S3Object = S3Object.builder().key(path).size(size).build
  }

  private val mockedObjects1 = Seq(
    MockedObjectDef("/dir/to/size/.hidden_file1.abc", 1L),
    MockedObjectDef("/dir/to/size/_hidden.file2.abc", 2L),
    MockedObjectDef("/dir/to/size/regular-file3.abc", 4L)
  ).map(_.toObject)

  private val mockedObjects2 = Seq(
    MockedObjectDef("/dir/to/size/.hidden_file10.abc", 10L),
    MockedObjectDef("/dir/to/size/_hidden.file20.abc", 20L),
    MockedObjectDef("/dir/to/size/regular-file30.gz", 40L)
  ).map(_.toObject)

  it should "get dir size - simple (no filtering, no pagination)" in {
    val f = fixture
    val path = "s3://bucket3/dir/to/size"

    val mockedListResponse: ListObjectsV2Response = ListObjectsV2Response.builder()
      .isTruncated(false)
      .contents(mockedObjects1.asJava)
      .build

    // mock S3 response
    Mockito.when(f.mockedS3Client.listObjectsV2(ArgumentMatchers.any[ListObjectsV2Request])).thenReturn(mockedListResponse)
    val dirSizeResult = f.mockedS3FsUtils.getDirectorySize(path)

    // verify request content
    val requestCaptor: Captor[ListObjectsV2Request] = ArgCaptor[ListObjectsV2Request]
    Mockito.verify(f.mockedS3Client).listObjectsV2(requestCaptor.capture)
    val capturedListRequest = requestCaptor.value

    capturedListRequest.bucket shouldBe "bucket3"
    capturedListRequest.prefix shouldBe "dir/to/size"
    capturedListRequest.continuationToken shouldBe null

    // verify returned value
    dirSizeResult shouldBe 7L
  }

  {
    val (f1, f2) = (fixture, fixture)
    Seq(
      (f1, "all files", (f1.mockedS3FsUtils.getDirectorySize(_)): String => Long, 77L),
      (f2, "only non-hidden", (f2.mockedS3FsUtils.getDirectorySizeNoHidden(_)): String => Long, 44L)
    )
  }.foreach { case (f, testCaseName, getSizeOp, expectedSize) =>

    it should s"get dir size for $testCaseName - with pagination listing" in {
      val path = "s3://bucket3b/dir/to/size"

      val mockedListResponses: Seq[ListObjectsV2Response] = Seq(
        ListObjectsV2Response.builder().isTruncated(true).nextContinuationToken("token1")
          .contents(mockedObjects1.asJava).build,
        ListObjectsV2Response.builder().isTruncated(false)
          .contents(mockedObjects2.asJava).build
      )

      // mock S3 responses
      Mockito.when(f.mockedS3Client.listObjectsV2(ArgumentMatchers.any[ListObjectsV2Request]))
        .thenReturn(mockedListResponses(0))
        .thenReturn(mockedListResponses(1))
      val dirSizeResult = getSizeOp(path)

      // verify request content
      val requestCaptor: Captor[ListObjectsV2Request] = ArgCaptor[ListObjectsV2Request]
      Mockito.verify(f.mockedS3Client, Mockito.times(2)).listObjectsV2(requestCaptor.capture)
      val capturedListRequests = requestCaptor.values

      // bucket & path should always be the same
      capturedListRequests.foreach(_.bucket shouldBe "bucket3b")
      capturedListRequests.foreach(_.prefix shouldBe "dir/to/size")

      // when truncated, the continuationToken was passed along to the next request to resume correctly
      capturedListRequests.map(_.continuationToken) shouldBe List(null, "token1")

      // verify returned value
      dirSizeResult shouldBe expectedSize
    }
  }

  Seq(
    ("non-splittable", mockedObjects2, true),
    ("splittable", mockedObjects1, false)
  ).foreach { case (testCaseName, mockedObjects, expectedNonSplitability) =>
    it should s"find the file list be $testCaseName (simple case, no pagination)" in {
      val f = fixture
      val path = "s3://bucket4/dir/to/split"

      val mockedListResponse: ListObjectsV2Response = ListObjectsV2Response.builder()
        .isTruncated(false)
        .contents(mockedObjects.asJava)
        .build

      // mock S3 response
      Mockito.when(f.mockedS3Client.listObjectsV2(ArgumentMatchers.any[ListObjectsV2Request])).thenReturn(mockedListResponse)
      val isNonSplittableResult = f.mockedS3FsUtils.isNonSplittable(path)

      // verify request content
      val requestCaptor: Captor[ListObjectsV2Request] = ArgCaptor[ListObjectsV2Request]
      Mockito.verify(f.mockedS3Client).listObjectsV2(requestCaptor.capture)
      val capturedListRequest = requestCaptor.value

      capturedListRequest.bucket shouldBe "bucket4"
      capturedListRequest.prefix shouldBe "dir/to/split"
      capturedListRequest.continuationToken shouldBe null

      // verify returned value
      isNonSplittableResult shouldBe expectedNonSplitability
    }
  }

  it should s"find the file list be non-splittable with breakOut" in {
    val f = fixture
    val path = "s3://bucket4b/dir/to/split"

    val mockedListResponses: Seq[ListObjectsV2Response] = Seq(
      ListObjectsV2Response.builder().isTruncated(true).nextContinuationToken("token1")
        .contents(mockedObjects1.asJava).build,
      ListObjectsV2Response.builder().isTruncated(true).nextContinuationToken("token2")
        .contents(mockedObjects2.asJava).build
    )

    // mock S3 responses: pretend that there could be a third response with objects, but it should not be reached
    // because non-splittable file was already found and the breakOut should prevent from further processing
    Mockito.when(f.mockedS3Client.listObjectsV2(ArgumentMatchers.any[ListObjectsV2Request]))
      .thenReturn(mockedListResponses(0))
      .thenReturn(mockedListResponses(1))
      .thenThrow(new IllegalStateException("Unwanted state - breakOut for non-splitability does not work"))
    val isNonSplittableResult = f.mockedS3FsUtils.isNonSplittable(path)

    // verify request content
    val requestCaptor: Captor[ListObjectsV2Request] = ArgCaptor[ListObjectsV2Request]
    Mockito.verify(f.mockedS3Client, Mockito.times(2)).listObjectsV2(requestCaptor.capture)
    val capturedListRequests = requestCaptor.values

    // bucket & path should always be the same
    capturedListRequests.foreach(_.bucket shouldBe "bucket4b")
    capturedListRequests.foreach(_.prefix shouldBe "dir/to/split")

    // when truncated, the continuationToken was passed along to the next request to resume correctly
    capturedListRequests.map(_.continuationToken) shouldBe List(null, "token1")

    // verify returned value
    isNonSplittableResult shouldBe true
  }

  it should s"delete files - with pagination listing" in {
    val f = fixture
    val path = "s3://bucket5/dir/to/delete"

    // mock S3 list responses
    val mockedListResponses: Seq[ListObjectsV2Response] = Seq(
      ListObjectsV2Response.builder().isTruncated(true).nextContinuationToken("token1")
        .contents(mockedObjects1.asJava).build,
      ListObjectsV2Response.builder().isTruncated(false)
        .contents(mockedObjects2.asJava).build
    )
    Mockito.when(f.mockedS3Client.listObjectsV2(ArgumentMatchers.any[ListObjectsV2Request]))
      .thenReturn(mockedListResponses(0))
      .thenReturn(mockedListResponses(1))

    // mock delete responses
    val mockedDeleteReponse = mock[DeleteObjectsResponse]
    Mockito.when(f.mockedS3Client.deleteObjects(ArgumentMatchers.any[DeleteObjectsRequest]))
      .thenReturn(mockedDeleteReponse)
    Mockito.when(mockedDeleteReponse.errors).thenReturn(List.empty[S3Error].asJava)

    f.mockedS3FsUtils.deleteDirectoryRecursively(path)

    // verify list request contents
    val listRequestCaptor: Captor[ListObjectsV2Request] = ArgCaptor[ListObjectsV2Request]
    Mockito.verify(f.mockedS3Client, Mockito.times(2)).listObjectsV2(listRequestCaptor.capture)
    val capturedListRequests = listRequestCaptor.values

    // bucket & path should always be the same
    capturedListRequests.foreach(_.bucket shouldBe "bucket5")
    capturedListRequests.foreach(_.prefix shouldBe "dir/to/delete")

    // when truncated, the continuationToken was passed along to the next request to resume correctly
    capturedListRequests.map(_.continuationToken) shouldBe List(null, "token1")

    // verify delete requests made
    val deleteRequestCaptor: Captor[DeleteObjectsRequest] = ArgCaptor[DeleteObjectsRequest]
    Mockito.verify(f.mockedS3Client, Mockito.times(2)).deleteObjects(deleteRequestCaptor.capture)
    val capturedDeleteRequests = deleteRequestCaptor.values

    capturedDeleteRequests.foreach(_.bucket shouldBe "bucket5")
    // the requests should hold the paths listed
    val deletedKeysRequested = capturedDeleteRequests.flatMap(_.delete.objects.asScala.map(_.key))
    deletedKeysRequested should contain theSameElementsInOrderAs (mockedObjects1 ++ mockedObjects2).map(_.key)
  }

  private val unrelatedVersionObjects = Seq(
    MockedObjectDef("publish/path/enceladus_info_date=2020-02-22/enceladus_info_version=aaaa/unrelated.file"),
    MockedObjectDef("publish/path/enceladus_info_date=2020-02-22/enceladus_info_version=-6/unrelated.file")
  ).map(_.toObject)

  Seq(
    ("unrelated objects", unrelatedVersionObjects),
    ("no objecdts", List.empty[S3Object])
  ).foreach { case (testCaseName, mockedObjects) =>
    it should s"find the latest version (simple case of $testCaseName - no recursion) to be 0" in {
      val f = fixture
      val path = "s3://bucket6/publish/path"
      val reportDate = "2020-02-22"

      // mock S3 list response
      val mockedListResponse = ListObjectsV2Response.builder().isTruncated(false)
        .contents(mockedObjects.asJava).build
      Mockito.when(f.mockedS3Client.listObjectsV2(ArgumentMatchers.any[ListObjectsV2Request]))
        .thenReturn(mockedListResponse)

      val lastestVersion = f.mockedS3FsUtils.getLatestVersion(path, reportDate)

      // verify request content
      val requestCaptor: Captor[ListObjectsV2Request] = ArgCaptor[ListObjectsV2Request]
      Mockito.verify(f.mockedS3Client).listObjectsV2(requestCaptor.capture)
      val capturedListRequests = requestCaptor.value

      // bucket & path should always be the same
      capturedListRequests.bucket shouldBe "bucket6"
      capturedListRequests.prefix shouldBe "publish/path/enceladus_info_date=2020-02-22/enceladus_info_version="

      // verify returned value
      lastestVersion shouldBe 0
    }
  }

  it should s"find the latest version (with recursion)" in {
    val f = fixture
    val path = "s3://bucket6b/publish/path"
    val reportDate = "2020-02-22"

    // mock S3 list responses
    val mockedObjectForVersionLookoup1 = Seq(
      MockedObjectDef("publish/path/enceladus_info_date=2020-02-22/enceladus_info_version=1/file.abc"),
      MockedObjectDef("publish/path/enceladus_info_date=2020-02-22/enceladus_info_version=2/file2.abc"),
      MockedObjectDef("publish/path/enceladus_info_date=2020-02-22/enceladus_info_version=BOGUS/bogus.file")
    ).map(_.toObject)

    val mockedObjectForVersionLookoup2 = Seq(
      MockedObjectDef("publish/path/enceladus_info_date=2020-02-22/enceladus_info_version=4/file.abc"),
      MockedObjectDef("publish/path/enceladus_info_date=2020-02-22/enceladus_info_version=6/.hidden.abc") // hidden = no problem
    ).map(_.toObject)

    val mockedListResponses: Seq[ListObjectsV2Response] = Seq(
      ListObjectsV2Response.builder().isTruncated(true).nextContinuationToken("token1")
        .contents(mockedObjectForVersionLookoup1.asJava).build,
      ListObjectsV2Response.builder().isTruncated(false)
        .contents(mockedObjectForVersionLookoup2.asJava).build
    )

    Mockito.when(f.mockedS3Client.listObjectsV2(ArgumentMatchers.any[ListObjectsV2Request]))
      .thenReturn(mockedListResponses(0))
      .thenReturn(mockedListResponses(1))
    val latestVersion = f.mockedS3FsUtils.getLatestVersion(path, reportDate)

    // verify request content
    val requestCaptor: Captor[ListObjectsV2Request] = ArgCaptor[ListObjectsV2Request]
    Mockito.verify(f.mockedS3Client, Mockito.times(2)).listObjectsV2(requestCaptor.capture)
    val capturedListRequests = requestCaptor.values

    // bucket & path should always be the same
    capturedListRequests.foreach(_.bucket shouldBe "bucket6b")
    capturedListRequests.foreach(_.prefix shouldBe "publish/path/enceladus_info_date=2020-02-22/enceladus_info_version=")

    // when truncated, the continuationToken was passed along to the next request to resume correctly
    capturedListRequests.map(_.continuationToken) shouldBe List(null, "token1")

    // verify returned value
    latestVersion shouldBe 6
  }

}
