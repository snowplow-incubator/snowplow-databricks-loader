/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.databricks.processing

import cats.implicits._
import cats.{Applicative, Foldable}
import cats.effect.{Async, Sync}
import cats.effect.kernel.Unique
import fs2.{Chunk, Pipe, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.parquet.schema.MessageType
import com.github.mjakubowski84.parquet4s.RowParquetRecord

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.Instant

import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.ListOfList
import com.snowplowanalytics.snowplow.loaders.transform.{NonAtomicFields, SchemaSubVersion, TabledEntity}
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.databricks.{Environment, Metrics}

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    implicit val lookup: RegistryLookup[F] = Http4sRegistryLookup(env.httpClient)
    val eventProcessingConfig              = EventProcessingConfig(EventProcessingConfig.NoWindowing)
    env.source.stream(eventProcessingConfig, eventProcessor(env))
  }

  /** Model used between stages of the processing pipeline */

  private case class ParseResult(
    events: List[Event],
    parseFailures: List[BadRow],
    countBytes: Long,
    token: Unique.Token
  )

  private case class Batched(
    events: ListOfList[Event],
    parseFailures: ListOfList[BadRow],
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    countBytes: Long,
    tokens: Vector[Unique.Token]
  )

  private case class Transformed(
    events: List[RowParquetRecord],
    schema: MessageType,
    badAccumulated: ListOfList[BadRow],
    loadTstamp: Instant,
    tokens: Vector[Unique.Token]
  )

  private case class Serialized(
    bytes: ByteArrayInputStream,
    goodCount: Long,
    loadTstamp: Instant,
    tokens: Vector[Unique.Token]
  )

  private def eventProcessor[F[_]: Async: RegistryLookup](
    env: Environment[F]
  ): EventProcessor[F] = { in =>
    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    in.through(setLatency(env.metrics))
      .through(parseBytes(badProcessor))
      .through(BatchUp.withTimeout(env.batching.maxBytes.toLong, env.batching.maxDelay))
      .through(transform(badProcessor, env))
      .through(sendFailedEvents(env))
      .through(setBadMetric(env))
      .through(writeToParquet(env))
      .through(sendToDatabricks(env))
      .through(setGoodMetric(env))
      .through(emitTokens)
  }

  private def setLatency[F[_]: Sync](metrics: Metrics[F]): Pipe[F, TokenedEvents, TokenedEvents] =
    _.evalTap {
      _.earliestSourceTstamp match {
        case Some(t) =>
          for {
            now <- Sync[F].realTime
            latencyMillis = now.toMillis - t.toEpochMilli
            _ <- metrics.setLatencyMillis(latencyMillis)
          } yield ()
        case None =>
          Applicative[F].unit
      }
    }

  /** Parse raw bytes into Event using analytics sdk */
  private def parseBytes[F[_]: Sync](badProcessor: BadRowProcessor): Pipe[F, TokenedEvents, ParseResult] =
    _.evalMap { case TokenedEvents(chunk, token, _) =>
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
      } yield ParseResult(events, badRows, numBytes, token)
    }

  /** Transform the Event into values compatible with parquet */
  private def transform[F[_]: Sync: RegistryLookup](badProcessor: BadRowProcessor, env: Environment[F]): Pipe[F, Batched, Transformed] =
    _.evalMap { case Batched(events, parseFailures, entities, numBytes, tokens) =>
      for {
        _ <- Logger[F].debug(s"Processing batch of size ${events.size} and $numBytes bytes")
        nonAtomicFields <- NonAtomicFields.resolveTypes[F](env.resolver, entities)
        loadTstamp <- Sync[F].realTimeInstant
        ParquetUtils.TransformResult(moreBad, good) <- ParquetUtils.transform[F](badProcessor, events, nonAtomicFields, loadTstamp)
        schema = ParquetSchema.forBatch(nonAtomicFields.fields.map(_.mergedField))
      } yield Transformed(good, schema, parseFailures.prepend(moreBad), loadTstamp, tokens)
    }

  private def writeToParquet[F[_]: Sync](env: Environment[F]): Pipe[F, Transformed, Serialized] = { in =>
    for {
      InMemoryFileSystem.Configured(hadoopConf, getBytes) <- Stream.resource(InMemoryFileSystem.configure(env.batching))
      batch <- in
      _ <- ParquetUtils.write[F](hadoopConf, env.compression, batch.schema, batch.events).unitary
      bytes <- Stream.eval(getBytes)
    } yield Serialized(bytes, batch.events.length.toLong, batch.loadTstamp, batch.tokens)
  }

  private def sendToDatabricks[F[_]: Async](env: Environment[F]): Pipe[F, Serialized, Serialized] =
    _.parEvalMap(env.batching.uploadConcurrency) { batch =>
      if (batch.goodCount > 0)
        env.databricks.upload(batch.bytes, batch.loadTstamp).as(batch)
      else
        batch.pure[F]
    }

  private def sendFailedEvents[F[_]: Applicative, A](env: Environment[F]): Pipe[F, Transformed, Transformed] =
    _.evalTap { batch =>
      if (batch.badAccumulated.nonEmpty) {
        val serialized = batch.badAccumulated.mapUnordered(_.compactByteArray)
        env.badSink.sinkSimple(serialized)
      } else Applicative[F].unit
    }

  private def setBadMetric[F[_], A](env: Environment[F]): Pipe[F, Transformed, Transformed] =
    _.evalTap { batch =>
      env.metrics.addBad(batch.badAccumulated.size)
    }

  private def setGoodMetric[F[_], A](env: Environment[F]): Pipe[F, Serialized, Serialized] =
    _.evalTap { batch =>
      env.metrics.addGood(batch.goodCount)
    }

  private def emitTokens[F[_]]: Pipe[F, Serialized, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private implicit def batchable: BatchUp.Batchable[ParseResult, Batched] = new BatchUp.Batchable[ParseResult, Batched] {
    def combine(b: Batched, a: ParseResult): Batched =
      Batched(
        events        = b.events.prepend(a.events),
        parseFailures = b.parseFailures.prepend(a.parseFailures),
        countBytes    = b.countBytes + a.countBytes,
        entities      = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_)) |+| b.entities,
        tokens        = b.tokens :+ a.token
      )
    def single(a: ParseResult): Batched = {
      val entities = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_))
      Batched(ListOfList.of(List(a.events)), ListOfList.of(List(a.parseFailures)), entities, a.countBytes, Vector(a.token))
    }
    def weightOf(a: ParseResult): Long =
      a.countBytes
  }

}
