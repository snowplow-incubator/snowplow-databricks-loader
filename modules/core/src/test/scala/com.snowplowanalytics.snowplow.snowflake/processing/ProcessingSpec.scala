/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.databricks.processing

import cats.effect.IO
import fs2.{Chunk, Stream}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.Instant
import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.databricks.MockEnvironment
import com.snowplowanalytics.snowplow.databricks.MockEnvironment.Action
import com.snowplowanalytics.snowplow.sources.TokenedEvents

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  The databricks loader should:
    Insert events to Databricks and ack the events $e1
    Emit BadRows when there are badly formatted events $e2
    Write good batches and bad events when input contains both $e3
    Set the latency metric based off the message timestamp $e4
  """

  def e1 =
    for {
      inputs <- generateEvents.take(2).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.AddedBadCountMetric(0),
        Action.UploadedFile,
        Action.AddedGoodCountMetric(4),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
      )
    )

  def e2 =
    for {
      inputs <- generateBadlyFormatted.take(3).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SentToBad(6),
        Action.AddedBadCountMetric(6),
        Action.AddedGoodCountMetric(0),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
      )
    )

  def e3 =
    for {
      bads <- generateBadlyFormatted.take(3).compile.toList
      goods <- generateEvents.take(3).compile.toList
      inputs = bads.zip(goods).map { case (bad, good) =>
                 TokenedEvents(bad.events ++ good.events, good.ack, None)
               }
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SentToBad(6),
        Action.AddedBadCountMetric(6),
        Action.UploadedFile,
        Action.AddedGoodCountMetric(6),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
      )
    )

  def e4 = {
    val messageTime = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime = Instant.parse("2023-10-24T10:00:42.123Z")

    val io = for {
      inputs <- generateEvents.take(2).compile.toList.map {
                  _.map {
                    _.copy(earliestSourceTstamp = Some(messageTime))
                  }
                }
      control <- MockEnvironment.build(inputs)
      _ <- IO.sleep(processTime.toEpochMilli.millis)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SetLatencyMetric(42123),
        Action.SetLatencyMetric(42123),
        Action.AddedBadCountMetric(0),
        Action.UploadedFile,
        Action.AddedGoodCountMetric(4),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
      )
    )

    TestControl.executeEmbed(io)

  }

}

object ProcessingSpec {

  def generateEvents: Stream[IO, TokenedEvents] =
    Stream.eval {
      for {
        ack <- IO.unique
        eventId1 <- IO.randomUUID
        eventId2 <- IO.randomUUID
        collectorTstamp <- IO.realTimeInstant
      } yield {
        val event1 = Event.minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0")
        val event2 = Event.minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
        val serialized = Chunk(event1, event2).map { e =>
          ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
        }
        TokenedEvents(serialized, ack, None)
      }
    }.repeat

  def generateBadlyFormatted: Stream[IO, TokenedEvents] =
    Stream.eval {
      IO.unique.map { token =>
        val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        TokenedEvents(serialized, token, None)
      }
    }.repeat

}
