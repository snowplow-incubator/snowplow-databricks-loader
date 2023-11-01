/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.databricks

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import org.http4s.client.Client
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import fs2.Stream

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.databricks.processing.DatabricksUploader
import com.snowplowanalytics.snowplow.runtime.AppInfo

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import java.nio.ByteBuffer
import java.time.Instant

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Int) extends Action
    case object UploadedFile extends Action

    case class AddedGoodCountMetric(count: Int) extends Action
    case class AddedBadCountMetric(count: Int) extends Action
    case class SetLatencyMetric(millis: Long) extends Action
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
        batching = Config.Batching(
          maxBytes          = 16000000,
          maxDelay          = 10.seconds,
          uploadConcurrency = 1
        ),
        compression = CompressionCodecName.SNAPPY
      )
      MockEnvironment(state, env)
    }

  val appInfo = new AppInfo {
    def name        = "databricks-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/databricks-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  private def testDatabricksUploader(state: Ref[IO, Vector[Action]]): DatabricksUploader[IO] = new DatabricksUploader[IO] {
    def upload(bytes: ByteBuffer, loadTstamp: Instant): IO[Unit] =
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

      def processingLatency: IO[FiniteDuration] = IO.pure(Duration.Zero)
    }

  private def testSink(ref: Ref[IO, Vector[Action]]): Sink[IO] = Sink[IO] { batch =>
    ref.update(_ :+ SentToBad(batch.size))
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] = new Metrics[IO] {
    def addBad(count: Int): IO[Unit] =
      ref.update(_ :+ AddedBadCountMetric(count))

    def addGood(count: Int): IO[Unit] =
      ref.update(_ :+ AddedGoodCountMetric(count))

    def setLatencyMillis(latencyMillis: Long): IO[Unit] =
      ref.update(_ :+ SetLatencyMetric(latencyMillis))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }
}
