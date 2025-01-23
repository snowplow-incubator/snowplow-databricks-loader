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
    Initialize the volume with an empty parquet file when there are no events $e0
    Insert events to Databricks and ack the events $e1
    Emit BadRows when there are badly formatted events $e2
    Write good batches and bad events when input contains both $e3
    Set the latency metric based off the message timestamp $e4
  """

  def e0 = {
    val io = for {
      control <- MockEnvironment.build(Nil)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SerializedToParquet(0),
        Action.UploadedFile("mock serialized 0 events")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e1 = {
    val io = for {
      inputs <- generateEvents.take(2).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SerializedToParquet(0),
        Action.UploadedFile("mock serialized 0 events"),
        Action.AddedBadCountMetric(0),
        Action.SerializedToParquet(4),
        Action.UploadedFile("mock serialized 4 events"),
        Action.AddedGoodCountMetric(4),
        Action.SetE2ELatencyMetric(0),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
      )
    )

    TestControl.executeEmbed(io)
  }

  def e2 = {
    val io = for {
      inputs <- generateBadlyFormatted.take(3).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SerializedToParquet(0),
        Action.UploadedFile("mock serialized 0 events"),
        Action.SentToBad(6),
        Action.AddedBadCountMetric(6),
        Action.SerializedToParquet(0),
        Action.AddedGoodCountMetric(0),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
      )
    )

    TestControl.executeEmbed(io)
  }

  def e3 = {
    val io = for {
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
        Action.SerializedToParquet(0),
        Action.UploadedFile("mock serialized 0 events"),
        Action.SentToBad(6),
        Action.AddedBadCountMetric(6),
        Action.SerializedToParquet(6),
        Action.UploadedFile("mock serialized 6 events"),
        Action.AddedGoodCountMetric(6),
        Action.SetE2ELatencyMetric(0),
        Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
      )
    )

    TestControl.executeEmbed(io)
  }

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
        Action.SerializedToParquet(0),
        Action.UploadedFile("mock serialized 0 events"),
        Action.SetLatencyMetric(42123),
        Action.SetLatencyMetric(42123),
        Action.AddedBadCountMetric(0),
        Action.SerializedToParquet(4),
        Action.UploadedFile("mock serialized 4 events"),
        Action.AddedGoodCountMetric(4),
        Action.SetE2ELatencyMetric(processTime.toEpochMilli),
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
