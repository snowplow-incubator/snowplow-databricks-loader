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
import com.github.mjakubowski84.parquet4s.{DateTimeValue, NullValue, TimestampFormat, Value}

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.Instant
import scala.concurrent.duration.{Duration, DurationLong}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.databricks.MockEnvironment
import com.snowplowanalytics.snowplow.databricks.MockEnvironment.Action
import com.snowplowanalytics.snowplow.streams.TokenedEvents

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = sequential ^ s2"""
  The databricks loader should:
    Initialize the volume with an empty parquet file when there are no events $e0
    Insert events to Databricks and ack the events $e1
    Emit BadRows when there are badly formatted events $e2
    Write good batches and bad events when input contains both $e3
  """

  def e0 = {
    val io = runTest(IO.pure(Nil)) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(Action.UploadedFile)
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e1 = {
    val collectorTstamp                = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime                    = Instant.parse("2023-10-24T10:00:52.123Z")
    val generator                      = good(collectorTstamp = Some(collectorTstamp))
    val expectedCollectorTstamp: Value = DateTimeValue(collectorTstamp.toEpochMilli * 1000, TimestampFormat.Int64Micros)

    val io = runTest(inputEvents(count = 2, generator)) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(4),
          Action.SetE2ELatencyMetric(52123.millis),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(4),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results.uploaded(1).map(_.get("etl_tstamp")) should contain(beSome[Value](NullValue)).forall,
          results.uploaded(1).map(_.get("collector_tstamp")) should contain(Some(expectedCollectorTstamp))
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e2 = {
    val io = runTest(inputEvents(count = 3, badlyFormatted)) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield results.actions should beEqualTo(
        Vector(
          Action.UploadedFile,
          Action.SentToBad(6),
          Action.AddedBadCountMetric(6),
          Action.AddedGoodCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }

    TestControl.executeEmbed(io)
  }

  def e3 = {
    val toInputs = for {
      bads <- inputEvents(count = 3, badlyFormatted)
      goods <- inputEvents(count = 3, good())
    } yield bads.zip(goods).map { case (bad, good) =>
      TokenedEvents(bad.events ++ good.events, good.ack)
    }

    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield results.actions should beEqualTo(
        Vector(
          Action.UploadedFile,
          Action.SentToBad(6),
          Action.AddedBadCountMetric(6),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(6),
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }

    TestControl.executeEmbed(io)
  }

}

object ProcessingSpec {

  def runTest[A](
    toInputs: IO[List[TokenedEvents]]
  )(
    f: (List[TokenedEvents], MockEnvironment) => IO[A]
  ): IO[A] =
    toInputs.flatMap { inputs =>
      MockEnvironment.build(inputs).use { control =>
        f(inputs, control)
      }
    }

  def inputEvents(count: Long, source: IO[TokenedEvents]): IO[List[TokenedEvents]] =
    Stream
      .eval(source)
      .repeat
      .take(count)
      .compile
      .toList

  def good(
    ue: UnstructEvent                = UnstructEvent(None),
    contexts: Contexts               = Contexts(List.empty),
    collectorTstamp: Option[Instant] = None
  ): IO[TokenedEvents] =
    for {
      ack <- IO.unique
      eventId1 <- IO.randomUUID
      eventId2 <- IO.randomUUID
      now <- IO.realTimeInstant
      tstamp = collectorTstamp.getOrElse(now)
    } yield {
      val event1 = Event.minimal(eventId1, tstamp, "0.0.0", "0.0.0").copy(unstruct_event = ue).copy(contexts = contexts)
      val event2 = Event.minimal(eventId2, tstamp, "0.0.0", "0.0.0")
      val serialized = Chunk(event1, event2).map { e =>
        ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
      }
      TokenedEvents(serialized, ack)
    }

  def badlyFormatted: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token)
    }

}
