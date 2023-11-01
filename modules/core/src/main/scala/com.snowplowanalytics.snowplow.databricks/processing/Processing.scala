/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.databricks.processing

import cats.implicits._
import cats.data.Validated
import cats.{Applicative, Foldable, Semigroup}
import cats.effect.{Async, Sync}
import cats.effect.kernel.Unique
import fs2.{Pipe, Pull, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.parquet.schema.MessageType
import com.github.mjakubowski84.parquet4s.RowParquetRecord

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import scala.concurrent.duration.Duration

import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.databricks.{Config, Environment, Metrics}
import com.snowplowanalytics.snowplow.loaders.transform.{NonAtomicFields, SchemaSubVersion, TabledEntity}

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    implicit val lookup: RegistryLookup[F] = Http4sRegistryLookup(env.httpClient)
    val eventProcessingConfig              = EventProcessingConfig(EventProcessingConfig.NoWindowing)
    env.source.stream(eventProcessingConfig, eventProcessor(env))
  }

  /** Model used between stages of the processing pipeline */

  private case class ParsedBatch(
    events: Vector[Event],
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    bad: List[BadRow],
    countBytes: Long,
    tokens: List[Unique.Token]
  )

  private case class Transformed(
    events: Vector[RowParquetRecord],
    schema: MessageType,
    badAccumulated: List[BadRow],
    tokens: List[Unique.Token]
  )

  private case class Serialized(
    bytes: ByteBuffer,
    goodCount: Int,
    tokens: List[Unique.Token]
  )

  private def eventProcessor[F[_]: Async: RegistryLookup](
    env: Environment[F]
  ): EventProcessor[F] = { in =>
    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    in.through(setLatency(env.metrics))
      .through(parseBytes(badProcessor))
      .through(batchUp(env.batching))
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
  private def parseBytes[F[_]: Sync](badProcessor: BadRowProcessor): Pipe[F, TokenedEvents, ParsedBatch] =
    _.evalMap { case TokenedEvents(list, token, _) =>
      Foldable[List].foldM(list, ParsedBatch(Vector.empty, Map.empty, Nil, 0L, List(token))) { case (acc, bytes) =>
        for {
          // order of these byte buffer operations is important
          numBytes <- Sync[F].delay(bytes.limit() - bytes.position())
          stringified <- Sync[F].delay(StandardCharsets.UTF_8.decode(bytes).toString)
        } yield Event.parse(stringified) match {
          case Validated.Valid(e) =>
            val te = TabledEntity.forEvent(e)
            acc.copy(events = e +: acc.events, countBytes = acc.countBytes + numBytes, entities = te |+| acc.entities)
          case Validated.Invalid(failure) =>
            val payload = BadRowRawPayload(stringified)
            val bad     = BadRow.LoaderParsingError(badProcessor, failure, payload)
            acc.copy(bad = bad :: acc.bad, countBytes = acc.countBytes + numBytes)
        }
      }
    }

  /** Transform the Event into values compatible with parquet */
  private def transform[F[_]: Sync: RegistryLookup](badProcessor: BadRowProcessor, env: Environment[F]): Pipe[F, ParsedBatch, Transformed] =
    _.evalMap { case ParsedBatch(events, entities, bad, numBytes, tokens) =>
      for {
        _ <- Logger[F].debug(s"Processing batch of size ${events.size} and $numBytes bytes")
        nonAtomicFields <- NonAtomicFields.resolveTypes[F](env.resolver, entities)
        now <- Sync[F].realTimeInstant
        ParquetUtils.TransformResult(moreBad, good) <- ParquetUtils.transform[F](badProcessor, events, nonAtomicFields, now)
        schema = ParquetSchema.forBatch(nonAtomicFields.fields.map(_.mergedField))
      } yield Transformed(good, schema, moreBad.toList ::: bad, tokens)
    }

  private def writeToParquet[F[_]: Sync](env: Environment[F]): Pipe[F, Transformed, Serialized] = { in =>
    for {
      InMemoryFileSystem.Configured(hadoopConf, getBytes) <- Stream.resource(InMemoryFileSystem.configure(env.batching))
      batch <- in
      _ <- ParquetUtils.write[F](hadoopConf, env.compression, batch.schema, batch.events).unitary
      bytes <- Stream.eval[F, ByteBuffer](getBytes)
    } yield Serialized(bytes, batch.events.length, batch.tokens)
  }

  private def sendToDatabricks[F[_]: Async](env: Environment[F]): Pipe[F, Serialized, Serialized] =
    _.parEvalMap(env.batching.uploadConcurrency) { batch =>
      if (batch.goodCount > 0)
        env.databricks.upload(batch.bytes).as(batch)
      else
        batch.pure[F]
    }

  private def sendFailedEvents[F[_]: Applicative, A](env: Environment[F]): Pipe[F, Transformed, Transformed] =
    _.evalTap { batch =>
      if (batch.badAccumulated.nonEmpty) {
        val serialized = batch.badAccumulated.map(_.compact.getBytes(StandardCharsets.UTF_8))
        env.badSink.sinkSimple(serialized)
      } else Applicative[F].unit
    }

  private def setBadMetric[F[_], A](env: Environment[F]): Pipe[F, Transformed, Transformed] =
    _.evalTap { batch =>
      env.metrics.addBad(batch.badAccumulated.length)
    }

  private def setGoodMetric[F[_], A](env: Environment[F]): Pipe[F, Serialized, Serialized] =
    _.evalTap { batch =>
      env.metrics.addGood(batch.goodCount)
    }

  private def emitTokens[F[_]]: Pipe[F, Serialized, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private implicit def parsedBatchSemigroup: Semigroup[ParsedBatch] = new Semigroup[ParsedBatch] {
    def combine(x: ParsedBatch, y: ParsedBatch): ParsedBatch =
      ParsedBatch(
        x.events ++ y.events,
        x.entities |+| y.entities,
        x.bad ::: y.bad,
        x.countBytes + y.countBytes,
        x.tokens ::: y.tokens
      )
  }

  private def batchUp[F[_]: Async](config: Config.Batching): Pipe[F, ParsedBatch, ParsedBatch] = {
    def go(
      timedPull: Pull.Timed[F, ParsedBatch],
      unflushed: Option[ParsedBatch]
    ): Pull[F, ParsedBatch, Unit] =
      timedPull.uncons.flatMap {
        case None => // Upstream stream has finished cleanly
          unflushed match {
            case None    => Pull.done
            case Some(b) => Pull.output1(b) *> Pull.done
          }
        case Some((Left(_), next)) => // The timer we set has timed out.
          unflushed match {
            case None    => go(next, None)
            case Some(b) => Pull.output1(b) >> go(next, None)
          }
        case Some((Right(pulled), next)) if pulled.isEmpty =>
          go(next, unflushed)
        case Some((Right(nonEmptyChunk), next)) => // Received another batch before the timer timed out
          val combined = unflushed match {
            case None    => nonEmptyChunk.iterator.reduce(_ |+| _)
            case Some(b) => nonEmptyChunk.iterator.foldLeft(b)(_ |+| _)
          }
          if (combined.countBytes > config.maxBytes)
            for {
              _ <- Pull.output1(combined)
              _ <- next.timeout(Duration.Zero)
              _ <- go(next, None)
            } yield ()
          else {
            for {
              _ <- if (unflushed.isEmpty) next.timeout(config.maxDelay) else Pull.pure(())
              _ <- go(next, Some(combined))
            } yield ()
          }
      }
    in =>
      in.pull.timed { timedPull =>
        go(timedPull, None)
      }.stream
  }

}
