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
import com.github.mjakubowski84.parquet4s.{ParquetReader, Path => Parquet4sPath, RowParquetRecord}
import fs2.Stream
import fs2.io.file.Files

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.streams.{
  EventProcessingConfig,
  EventProcessor,
  ListOfList,
  Sink,
  Sinkable,
  SourceAndAck,
  TokenedEvents
}
import com.snowplowanalytics.snowplow.databricks.processing.{DatabricksUploader, ParquetSerializer}
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import java.io.ByteArrayInputStream

case class MockEnvironment(state: Ref[IO, MockEnvironment.Results], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Long) extends Action
    case object UploadedFile extends Action
    case class AddedGoodCountMetric(count: Long) extends Action
    case class AddedBadCountMetric(count: Long) extends Action
    case class SetLatencyMetric(millis: FiniteDuration) extends Action
    case class SetE2ELatencyMetric(millis: FiniteDuration) extends Action
    case object IncrementedDatabricksErrors extends Action
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
  }
  import Action._

  /**
   * The state recorded by the mock environment
   * @param actions
   *   The Actions that happened to this mock environment during a test
   * @param uploaded
   *   The events that this mock environment uploaded during a test
   */
  case class Results(actions: Vector[Action], uploaded: Vector[Vector[RowParquetRecord]]) {
    def withAction(action: Action): Results =
      copy(actions = actions :+ action)
  }

  /**
   * Build a mock environment for testing
   *
   * @param inputs
   *   Input events to send into the environment.
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(inputs: List[TokenedEvents]): Resource[IO, MockEnvironment] =
    for {
      state <- Resource.eval(Ref[IO].of(Results(Vector.empty, Vector.empty)))
      batching = Config.Batching(
                   maxBytes                = 16000000,
                   maxDelay                = 10.seconds,
                   uploadParallelismFactor = 1
                 )
      serializer <- ParquetSerializer.resource[IO](batching, CompressionCodecName.SNAPPY)
    } yield {
      val env = Environment(
        appInfo                 = appInfo,
        source                  = testSourceAndAck(inputs, state),
        badSink                 = testSink(state),
        resolver                = Resolver[IO](Nil, None),
        httpClient              = testHttpClient,
        databricks              = testDatabricksUploader(state),
        metrics                 = testMetrics(state),
        appHealth               = testAppHealth(state),
        serializer              = serializer,
        batching                = batching,
        cpuParallelism          = 1,
        uploadParallelism       = 1,
        badRowMaxSize           = 1000000,
        schemasToSkip           = List.empty,
        exitOnMissingIgluSchema = false,
        devFeatures             = Config.DevFeatures(false)
      )
      MockEnvironment(state, env)
    }

  val appInfo = new AppInfo {
    def name        = "databricks-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/databricks-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  private def testDatabricksUploader(state: Ref[IO, Results]): DatabricksUploader.WithHandledErrors[IO] =
    new DatabricksUploader.WithHandledErrors[IO] {
      def upload(bytes: ByteArrayInputStream): IO[Unit] =
        Files[IO].tempFile.use { path =>
          val length    = bytes.available
          val byteArray = new Array[Byte](length)

          val copyToByteArray = IO.delay(bytes.read(byteArray, 0, length))

          val writeToTempFile = Stream
            .emits(byteArray)
            .through(Files[IO].writeAll(path))
            .compile
            .drain

          for {
            _ <- copyToByteArray
            _ <- writeToTempFile
            contents <- IO.blocking(ParquetReader.generic.read(Parquet4sPath(path.toNioPath)).toVector)
            _ <- state.update { case Results(actions, uploaded) =>
                   Results(actions :+ UploadedFile, uploaded :+ contents)
                 }
          } yield ()
        }
    }

  private def testSourceAndAck(inputs: List[TokenedEvents], state: Ref[IO, Results]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream
          .emits(inputs)
          .through(processor)
          .chunks
          .evalMap { chunk =>
            state.update(_.withAction(Checkpointed(chunk.toList)))
          }
          .drain

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] =
        IO.pure(None)
    }

  private def testSink(ref: Ref[IO, Results]): Sink[IO] = new Sink[IO] {
    override def sink(batch: ListOfList[Sinkable]): IO[Unit] =
      ref.update(_.withAction(SentToBad(batch.size)))

    override def isHealthy: IO[Boolean] = IO.pure(true)
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  def testMetrics(ref: Ref[IO, Results]): Metrics[IO] = new Metrics[IO] {
    def addBad(count: Long): IO[Unit] =
      ref.update(_.withAction(AddedBadCountMetric(count)))

    def addGood(count: Long): IO[Unit] =
      ref.update(_.withAction(AddedGoodCountMetric(count)))

    def setLatency(latency: FiniteDuration): IO[Unit] =
      ref.update(_.withAction(SetLatencyMetric(latency)))

    def setE2ELatency(latency: FiniteDuration): IO[Unit] =
      ref.update(_.withAction(SetE2ELatencyMetric(latency)))

    def incrementDatabricksErrors(): IO[Unit] =
      ref.update(_.withAction(IncrementedDatabricksErrors))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }

  private def testAppHealth(ref: Ref[IO, Results]): AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        IO.unit
      def beUnhealthyForSetup(alert: Alert): IO[Unit] =
        IO.unit
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(_.withAction(BecameHealthy(service)))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(_.withAction(BecameUnhealthy(service)))
    }

}
