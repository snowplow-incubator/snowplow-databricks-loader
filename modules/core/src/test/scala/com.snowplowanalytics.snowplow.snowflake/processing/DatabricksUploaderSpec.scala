/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.databricks.processing

import cats.effect.IO
import cats.effect.kernel.Ref
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import com.databricks.sdk.core.ApiClient
import com.databricks.sdk.core.error.platform.AlreadyExists
import com.databricks.sdk.service.files.{FilesAPI, UploadRequest}

import java.io.ByteArrayInputStream

import com.snowplowanalytics.snowplow.databricks.MockEnvironment

class DatabricksUploaderSpec extends Specification with CatsEffect {
  import DatabricksUploaderSpec._

  def is = s2"""
  The DatabricksUploader should:
    Increment databricks_errors metric when upload fails with non-AlreadyExists exception $e1
    Not increment databricks_errors metric when upload succeeds $e2
    Not increment databricks_errors metric when upload fails with AlreadyExists exception $e3
  """

  def e1 = {
    val testException = new RuntimeException("Databricks API failure")

    for {
      state <- Ref[IO].of(MockEnvironment.Results(Vector.empty, Vector.empty))
      metrics  = MockEnvironment.testMetrics(state)
      api      = failingFilesAPI(testException)
      uploader = DatabricksUploader.impl(api, metrics)
      result <- uploader.upload(testBytes, "test-path").attempt
      finalState <- state.get
      errorCount = finalState.actions.count(_ == MockEnvironment.Action.IncrementedDatabricksErrors)
    } yield List(
      result.isLeft must beTrue,
      result.left.toOption must beSome(testException),
      errorCount must beEqualTo(1)
    ).reduce(_ and _)
  }

  def e2 =
    for {
      state <- Ref[IO].of(MockEnvironment.Results(Vector.empty, Vector.empty))
      metrics  = MockEnvironment.testMetrics(state)
      api      = successfulFilesAPI
      uploader = DatabricksUploader.impl(api, metrics)
      result <- uploader.upload(testBytes, "test-path").attempt
      finalState <- state.get
    } yield List(
      result.isRight must beTrue,
      finalState.actions must not(contain(MockEnvironment.Action.IncrementedDatabricksErrors: MockEnvironment.Action))
    ).reduce(_ and _)

  def e3 = {
    // Create AlreadyExists exception with null ErrorDetails (acceptable for testing)
    val alreadyExistsException = new AlreadyExists("File already exists", null)

    for {
      state <- Ref[IO].of(MockEnvironment.Results(Vector.empty, Vector.empty))
      metrics  = MockEnvironment.testMetrics(state)
      api      = failingFilesAPI(alreadyExistsException)
      uploader = DatabricksUploader.impl(api, metrics)
      result <- uploader.upload(testBytes, "test-path").attempt
      finalState <- state.get
    } yield List(
      result.isRight must beTrue, // AlreadyExists is recovered, not an error
      finalState.actions must not(contain(MockEnvironment.Action.IncrementedDatabricksErrors: MockEnvironment.Action))
    ).reduce(_ and _)
  }
}

object DatabricksUploaderSpec {

  def testBytes: ByteArrayInputStream =
    new ByteArrayInputStream(Array[Byte](1, 2, 3))

  /** Mock FilesAPI that succeeds */
  def successfulFilesAPI: FilesAPI = new FilesAPI(null: ApiClient) {
    override def upload(request: UploadRequest): Unit = ()
  }

  /** Mock FilesAPI that throws the given exception */
  def failingFilesAPI(exception: Throwable): FilesAPI = new FilesAPI(null: ApiClient) {
    override def upload(request: UploadRequest): Unit = throw exception
  }
}
