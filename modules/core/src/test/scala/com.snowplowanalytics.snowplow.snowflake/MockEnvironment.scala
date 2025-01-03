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
package com.snowplowanalytics.snowplow.databricks

import cats.implicits._
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import org.http4s.client.Client
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import fs2.Stream

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.databricks.processing.DatabricksUploader
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import java.io.ByteArrayInputStream

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Long) extends Action
    case object UploadedFile extends Action
    case class AddedGoodCountMetric(count: Long) extends Action
    case class AddedBadCountMetric(count: Long) extends Action
    case class SetLatencyMetric(millis: Long) extends Action
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
  }
  import Action._

  /**
   * Build a mock environment for testing
   *
   * @param inputs
   *   Input events to send into the environment.
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(inputs: List[TokenedEvents]): IO[MockEnvironment] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
    } yield {
      val env = Environment(
        appInfo    = appInfo,
        source     = testSourceAndAck(inputs, state),
        badSink    = testSink(state),
        resolver   = Resolver[IO](Nil, None),
        httpClient = testHttpClient,
        databricks = testDatabricksUploader(state),
        metrics    = testMetrics(state),
        appHealth  = testAppHealth(state),
        batching = Config.Batching(
          maxBytes          = 16000000,
          maxDelay          = 10.seconds,
          uploadConcurrency = 1
        ),
        compression             = CompressionCodecName.SNAPPY,
        badRowMaxSize           = 1000000,
        schemasToSkip           = List.empty,
        exitOnMissingIgluSchema = false
      )
      MockEnvironment(state, env)
    }

  val appInfo = new AppInfo {
    def name        = "databricks-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/databricks-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  private def testDatabricksUploader(state: Ref[IO, Vector[Action]]): DatabricksUploader.WithHandledErrors[IO] =
    new DatabricksUploader.WithHandledErrors[IO] {
      def upload(bytes: ByteArrayInputStream): IO[Unit] =
        state.update(_ :+ UploadedFile)
    }

  private def testSourceAndAck(inputs: List[TokenedEvents], state: Ref[IO, Vector[Action]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream
          .emits(inputs)
          .through(processor)
          .chunks
          .evalMap { chunk =>
            state.update(_ :+ Checkpointed(chunk.toList))
          }
          .drain

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] =
        IO.pure(None)
    }

  private def testSink(ref: Ref[IO, Vector[Action]]): Sink[IO] = Sink[IO] { batch =>
    ref.update(_ :+ SentToBad(batch.size))
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] = new Metrics[IO] {
    def addBad(count: Long): IO[Unit] =
      ref.update(_ :+ AddedBadCountMetric(count))

    def addGood(count: Long): IO[Unit] =
      ref.update(_ :+ AddedGoodCountMetric(count))

    def setLatencyMillis(latencyMillis: Long): IO[Unit] =
      ref.update(_ :+ SetLatencyMetric(latencyMillis))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }

  private def testAppHealth(ref: Ref[IO, Vector[Action]]): AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        IO.unit
      def beUnhealthyForSetup(alert: Alert): IO[Unit] =
        IO.unit
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(_ :+ BecameHealthy(service))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(_ :+ BecameUnhealthy(service))
    }
}
