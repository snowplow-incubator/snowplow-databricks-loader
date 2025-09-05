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

import cats.implicits._
import cats.{Applicative, Foldable, Monad}
import cats.effect.{Async, Sync}
import cats.effect.kernel.Unique
import fs2.{Chunk, Pipe, Stream}
import io.circe.syntax._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.parquet.schema.MessageType
import com.github.mjakubowski84.parquet4s.RowParquetRecord

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, ListOfList, TokenedEvents}
import com.snowplowanalytics.snowplow.loaders.transform.{BadRowsSerializer, NonAtomicFields, SchemaSubVersion, TabledEntity}
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.databricks.{Environment, RuntimeService}

object Processing {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    implicit val lookup: RegistryLookup[F] = Http4sRegistryLookup(env.httpClient)
    val eventProcessingConfig              = EventProcessingConfig(EventProcessingConfig.NoWindowing, env.metrics.setLatency)
    Stream.eval(initializeWithEmptyParquet(env)).drain ++
      env.source.stream(eventProcessingConfig, eventProcessor(env))
  }

  /** Model used between stages of the processing pipeline */

  private case class ParseResult(
    events: List[Event],
    parseFailures: List[BadRow],
    countBytes: Long,
    token: Unique.Token,
    earliestCollectorTstamp: Option[Instant]
  )

  private case class Batched(
    events: ListOfList[Event],
    parseFailures: ListOfList[BadRow],
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    countBytes: Long,
    tokens: Vector[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  private case class Transformed(
    events: List[RowParquetRecord],
    schema: MessageType,
    badAccumulated: ListOfList[BadRow],
    tokens: Vector[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  private case class Serialized(
    bytes: ByteArrayInputStream,
    goodCount: Long,
    tokens: Vector[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  private def eventProcessor[F[_]: Async: RegistryLookup](
    env: Environment[F]
  ): EventProcessor[F] = { in =>
    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    in.through(parseBytes(badProcessor, env))
      .through(BatchUp.withTimeout(env.batching.maxBytes.toLong, env.batching.maxDelay))
      .through(transform(badProcessor, env))
      .through(sendFailedEvents(env, badProcessor))
      .through(setBadMetric(env))
      .through(writeToParquet(env))
      .through(sendToDatabricks(env))
      .through(setPostLoadMetrics(env))
      .through(emitTokens)
  }

  /** Parse raw bytes into Event using analytics sdk */
  private def parseBytes[F[_]: Async](badProcessor: BadRowProcessor, env: Environment[F]): Pipe[F, TokenedEvents, ParseResult] =
    _.parEvalMap(env.cpuParallelism) { case TokenedEvents(chunk, token) =>
      for {
        numBytes <- Sync[F].delay(Foldable[Chunk].sumBytes(chunk))
        (badRows, events) <- Foldable[Chunk].traverseSeparateUnordered(chunk) { byteBuffer =>
                               Sync[F].delay {
                                 Event.parseBytes(byteBuffer).toEither.leftMap { failure =>
                                   val payload = BadRowRawPayload(StandardCharsets.UTF_8.decode(byteBuffer).toString)
                                   BadRow.LoaderParsingError(badProcessor, failure, payload)
                                 }
                               }
                             }
        earliestCollectorTstamp = events.view.map(_.collector_tstamp).minOption
      } yield ParseResult(events, badRows, numBytes, token, earliestCollectorTstamp)
    }

  /** Transform the Event into values compatible with parquet */
  private def transform[F[_]: Async: RegistryLookup](badProcessor: BadRowProcessor, env: Environment[F]): Pipe[F, Batched, Transformed] =
    _.parEvalMap(env.cpuParallelism) { case Batched(events, parseFailures, entities, numBytes, tokens, earliestTstamp) =>
      for {
        _ <- Logger[F].debug(s"Processing batch of size ${events.size} and $numBytes bytes")
        resolveTypesResult <- NonAtomicFields.resolveTypes[F](env.resolver, entities, env.schemasToSkip)
        nonAtomicFields <- possiblyExitOnClashingIgluSchemas(env, resolveTypesResult)
        _ <- possiblyExitOnMissingIgluSchema(env, nonAtomicFields)
        TransformUtils.TransformResult(moreBad, good) <- TransformUtils.transform[F](badProcessor, events, nonAtomicFields, env.devFeatures)
        schema = ParquetSchema.forBatch(nonAtomicFields.fields.map(_.mergedField))
      } yield Transformed(good, schema, parseFailures.prepend(moreBad), tokens, earliestTstamp)
    }

  private def writeToParquet[F[_]: Sync](env: Environment[F]): Pipe[F, Transformed, Serialized] =
    _.evalMap { batch =>
      for {
        bytes <- env.serializer.serialize(batch.schema, batch.events)
      } yield Serialized(bytes, batch.events.length.toLong, batch.tokens, batch.earliestCollectorTstamp)
    }

  private def sendToDatabricks[F[_]: Async](env: Environment[F]): Pipe[F, Serialized, Serialized] =
    _.parEvalMap(env.uploadParallelism) { batch =>
      if (batch.goodCount > 0)
        env.databricks.upload(batch.bytes).as(batch)
      else
        batch.pure[F]
    }

  private def sendFailedEvents[F[_]: Sync](
    env: Environment[F],
    badRowProcessor: BadRowProcessor
  ): Pipe[F, Transformed, Transformed] =
    _.evalTap { batch =>
      if (batch.badAccumulated.nonEmpty) {
        val serialized =
          batch.badAccumulated.mapUnordered(badRow => BadRowsSerializer.withMaxSize(badRow, badRowProcessor, env.badRowMaxSize))
        env.badSink.sinkSimple(serialized)
      } else Applicative[F].unit
    }

  private def setBadMetric[F[_], A](env: Environment[F]): Pipe[F, Transformed, Transformed] =
    _.evalTap { batch =>
      env.metrics.addBad(batch.badAccumulated.size)
    }

  private def setPostLoadMetrics[F[_]: Sync](env: Environment[F]): Pipe[F, Serialized, Serialized] =
    _.evalTap { batch =>
      for {
        now <- Sync[F].realTime
        _ <- env.metrics.addGood(batch.goodCount)
        _ <- batch.earliestCollectorTstamp match {
               case Some(earliestCollectorTstamp) =>
                 env.metrics.setE2ELatency(now - earliestCollectorTstamp.toEpochMilli.millis)
               case None =>
                 Sync[F].unit
             }
      } yield ()
    }

  private def emitTokens[F[_]]: Pipe[F, Serialized, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private implicit def batchable: BatchUp.Batchable[ParseResult, Batched] = new BatchUp.Batchable[ParseResult, Batched] {
    def combine(b: Batched, a: ParseResult): Batched =
      Batched(
        events                  = b.events.prepend(a.events),
        parseFailures           = b.parseFailures.prepend(a.parseFailures),
        countBytes              = b.countBytes + a.countBytes,
        entities                = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_)) |+| b.entities,
        tokens                  = b.tokens :+ a.token,
        earliestCollectorTstamp = chooseEarliestTstamp(a.earliestCollectorTstamp, b.earliestCollectorTstamp)
      )
    def single(a: ParseResult): Batched = {
      val entities = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_))
      Batched(
        ListOfList.of(List(a.events)),
        ListOfList.of(List(a.parseFailures)),
        entities,
        a.countBytes,
        Vector(a.token),
        a.earliestCollectorTstamp
      )
    }
    def weightOf(a: ParseResult): Long =
      a.countBytes
  }

  private def possiblyExitOnMissingIgluSchema[F[_]: Sync](
    env: Environment[F],
    nonAtomicFields: NonAtomicFields.Result
  ): F[Unit] =
    if (env.exitOnMissingIgluSchema && nonAtomicFields.igluFailures.nonEmpty) {
      val base =
        "Exiting because failed to resolve Iglu schemas.  Either check the configuration of the Iglu repos, or set the `skipSchemas` config option, or set `exitOnMissingIgluSchema` to false.\n"
      val msg = nonAtomicFields.igluFailures.map(_.failure.asJson.noSpaces).mkString(base, "\n", "")
      Logger[F].error(base) *> env.appHealth.beUnhealthyForRuntimeService(RuntimeService.Iglu) *> Sync[F].raiseError(
        new RuntimeException(msg)
      )
    } else Applicative[F].unit

  private def chooseEarliestTstamp(o1: Option[Instant], o2: Option[Instant]): Option[Instant] =
    (o1, o2)
      .mapN { case (t1, t2) =>
        if (t1.isBefore(t2)) t1 else t2
      }
      .orElse(o1)
      .orElse(o2)

  private def initializeWithEmptyParquet[F[_]: Monad](env: Environment[F]): F[Unit] = {
    val schema = ParquetSchema.forBatch(Vector.empty)
    for {
      serialized <- env.serializer.serialize(schema, Nil)
      _ <- env.databricks.upload(serialized)
    } yield ()
  }

  private def possiblyExitOnClashingIgluSchemas[F[_]: Sync](
    env: Environment[F],
    resolveTypesResult: Either[NonAtomicFields.ResolveTypesException, NonAtomicFields.Result]
  ): F[NonAtomicFields.Result] =
    resolveTypesResult match {
      case Left(e) =>
        Logger[F].error(e.getMessage) *> env.appHealth.beUnhealthyForRuntimeService(RuntimeService.Iglu) *> Sync[F].raiseError(e)
      case Right(r) => Applicative[F].pure(r)
    }
}
