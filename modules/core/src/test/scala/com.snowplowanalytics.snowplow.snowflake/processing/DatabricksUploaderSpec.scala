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

import java.io.ByteArrayInputStream
import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Ref
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.databricks.{Alert, RuntimeService}
import com.snowplowanalytics.snowplow.databricks.{Config, MockEnvironment}

class DatabricksUploaderSpec extends Specification with CatsEffect {
  import DatabricksUploaderSpec._

  def is = s2"""
  The DatabricksUploader.withHandledErrors should:
    Increment databricks_errors metric when upload fails $e1
    Not increment databricks_errors metric when upload succeeds $e2
    Increment databricks_errors metric once per retry attempt $e3
  """

  def e1 = {
    val testException = new RuntimeException("Databricks API failure")

    for {
      state <- Ref[IO].of(MockEnvironment.Results(Vector.empty, Vector.empty))
      metrics    = MockEnvironment.testMetrics(state)
      underlying = failingUploader(testException)
      uploader   = DatabricksUploader.withHandledErrors(underlying, testAppHealth, testConfig, testRetries, metrics)
      result <- uploader.upload(testBytes).attempt
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
      metrics    = MockEnvironment.testMetrics(state)
      underlying = successfulUploader
      uploader   = DatabricksUploader.withHandledErrors(underlying, testAppHealth, testConfig, testRetries, metrics)
      result <- uploader.upload(testBytes).attempt
      finalState <- state.get
    } yield List(
      result.isRight must beTrue,
      finalState.actions must not(contain(MockEnvironment.Action.IncrementedDatabricksErrors: MockEnvironment.Action))
    ).reduce(_ and _)

  def e3 =
    for {
      state <- Ref[IO].of(MockEnvironment.Results(Vector.empty, Vector.empty))
      metrics            = MockEnvironment.testMetrics(state)
      failureThenSuccess = failingNTimesThenSuccessUploader(3)
      uploader = DatabricksUploader.withHandledErrors(failureThenSuccess, testAppHealth, testConfig, retriesWithAttempts(5), metrics)
      result <- uploader.upload(testBytes).attempt
      finalState <- state.get
      errorCount = finalState.actions.count(_ == MockEnvironment.Action.IncrementedDatabricksErrors)
    } yield List(
      result.isRight must beTrue, // Eventually succeeds after retries
      errorCount must beEqualTo(3) // Should count each failed attempt
    ).reduce(_ and _)
}

object DatabricksUploaderSpec {

  def testBytes: ByteArrayInputStream =
    new ByteArrayInputStream(Array[Byte](1, 2, 3))

  def testConfig: Config.Databricks = Config.Databricks(
    host        = "test.databricks.com",
    token       = None,
    oauth       = None,
    catalog     = "test_catalog",
    schema      = "test_schema",
    volume      = "test_volume",
    compression = org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY,
    httpTimeout = 10.seconds
  )

  def testRetries: Config.Retries = Config.Retries(
    transientErrors = Retrying.Config.ForTransient(1.milli, 1),
    setupErrors     = Retrying.Config.ForSetup(1.milli)
  )

  def retriesWithAttempts(attempts: Int): Config.Retries = Config.Retries(
    transientErrors = Retrying.Config.ForTransient(1.milli, attempts),
    setupErrors     = Retrying.Config.ForSetup(1.milli)
  )

  def testAppHealth: AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit]                                     = IO.unit
      def beUnhealthyForSetup(alert: Alert): IO[Unit]                     = IO.unit
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit]   = IO.unit
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] = IO.unit
    }

  /** Mock DatabricksUploader that succeeds */
  def successfulUploader: DatabricksUploader[IO] = new DatabricksUploader[IO] {
    def upload(bytes: ByteArrayInputStream, path: String): IO[Unit] = IO.unit
  }

  /** Mock DatabricksUploader that throws the given exception */
  def failingUploader(exception: Throwable): DatabricksUploader[IO] = new DatabricksUploader[IO] {
    def upload(bytes: ByteArrayInputStream, path: String): IO[Unit] = IO.raiseError(exception)
  }

  /** Mock DatabricksUploader that fails N times then succeeds */
  def failingNTimesThenSuccessUploader(failureCount: Int): DatabricksUploader[IO] = new DatabricksUploader[IO] {
    private val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    def upload(bytes: ByteArrayInputStream, path: String): IO[Unit] = {
      val attempt = counter.incrementAndGet()
      if (attempt <= failureCount) {
        IO.raiseError(new RuntimeException(s"Databricks API failure (attempt $attempt)"))
      } else {
        IO.unit
      }
    }
  }
}
