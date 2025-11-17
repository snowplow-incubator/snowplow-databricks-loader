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
import com.databricks.sdk.core.DatabricksException
import com.databricks.sdk.core.error.platform.Unauthenticated

class DatabricksUploaderSpec extends Specification with CatsEffect {
  import DatabricksUploaderSpec._

  def is = s2"""
  The DatabricksUploader.withHandledErrors should:
    Increment databricks_errors metric when upload fails $e1
    Not increment databricks_errors metric when upload succeeds $e2
    Increment databricks_errors metric once per retry attempt $e3
    Increment setup_errors metric when setup error occurs $e4
    Include attemptCounter in filename for each retry attempt $e5
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
      result must beLeft(testException),
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
      result must beRight,
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
      result must beRight, // Eventually succeeds after retries
      errorCount must beEqualTo(3) // Should count each failed attempt
    ).reduce(_ and _)

  def e4 = {
    val wrappedException =
      new DatabricksException("oauth-m2m auth: Failed to refresh credentials: invalid_client", new Unauthenticated("invalid_client", null))

    for {
      state <- Ref[IO].of(MockEnvironment.Results(Vector.empty, Vector.empty))
      metrics = MockEnvironment.testMetrics(state)
      // Fail once with wrapped exception, then succeed
      underlying = failingNTimesThenSuccessUploader(1, Some(wrappedException))
      uploader   = DatabricksUploader.withHandledErrors(underlying, testAppHealth, testConfig, testRetries, metrics)
      result <- uploader.upload(testBytes).attempt
      finalState <- state.get
      setupErrorCount      = finalState.actions.count(_ == MockEnvironment.Action.IncrementedSetupErrors)
      databricksErrorCount = finalState.actions.count(_ == MockEnvironment.Action.IncrementedDatabricksErrors)
    } yield List(
      result must beRight, // Should eventually succeed after retry
      setupErrorCount must beEqualTo(1),
      databricksErrorCount must beEqualTo(0)
    ).reduce(_ and _)
  }

  def e5 =
    for {
      state <- Ref[IO].of(MockEnvironment.Results(Vector.empty, Vector.empty))
      pathsRef <- Ref[IO].of(List.empty[String])
      metrics = MockEnvironment.testMetrics(state)
      // Fail 3 times, capturing paths along the way
      underlying <- pathCapturingUploader(pathsRef, failureCount = 3)
      uploader = DatabricksUploader.withHandledErrors(underlying, testAppHealth, testConfig, retriesWithAttempts(5), metrics)
      result <- uploader.upload(testBytes).attempt
      capturedPaths <- pathsRef.get
      // Extract everything except the attempt counter from each path
      pathsWithoutCounter = capturedPaths.map { path =>
                              // Replace the counter (e.g., "-0.snappy.parquet", "-1.snappy.parquet") with a placeholder
                              path.replaceAll("-\\d+\\.snappy\\.parquet$", "-X.snappy.parquet")
                            }
    } yield List(
      result must beRight, // Eventually succeeds after retries
      capturedPaths.size must beEqualTo(4), // 3 failures + 1 success
      // Check that attemptCounter is included in each filename
      capturedPaths(0) must contain("-0.snappy.parquet"), // First attempt (counter = 0)
      capturedPaths(1) must contain("-1.snappy.parquet"), // Second attempt (counter = 1)
      capturedPaths(2) must contain("-2.snappy.parquet"), // Third attempt (counter = 2)
      capturedPaths(3) must contain("-3.snappy.parquet"), // Fourth attempt (counter = 3, succeeds)
      // Verify all paths are identical except for the attempt counter
      pathsWithoutCounter.distinct.size must beEqualTo(1)
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
  def failingNTimesThenSuccessUploader(failureCount: Int, exception: Option[Throwable] = None): DatabricksUploader[IO] =
    new DatabricksUploader[IO] {
      private val counter = new java.util.concurrent.atomic.AtomicInteger(0)
      def upload(bytes: ByteArrayInputStream, path: String): IO[Unit] = {
        val attempt = counter.incrementAndGet()
        if (attempt <= failureCount) {
          val ex = exception.getOrElse(new RuntimeException(s"Databricks API failure (attempt $attempt)"))
          IO.raiseError(ex)
        } else {
          IO.unit
        }
      }
    }

  /** Mock DatabricksUploader that captures paths and fails N times then succeeds */
  def pathCapturingUploader(pathsRef: Ref[IO, List[String]], failureCount: Int): IO[DatabricksUploader[IO]] =
    Ref[IO].of(0).map { counterRef =>
      new DatabricksUploader[IO] {
        def upload(bytes: ByteArrayInputStream, path: String): IO[Unit] =
          for {
            attempt <- counterRef.updateAndGet(_ + 1)
            _ <- pathsRef.update(_ :+ path)
            _ <- if (attempt <= failureCount) {
                   IO.raiseError(new RuntimeException(s"Databricks API failure (attempt $attempt)"))
                 } else {
                   IO.unit
                 }
          } yield ()
      }
    }
}
