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
import com.github.mjakubowski84.parquet4s._

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.Instant
import scala.concurrent.duration.{Duration, DurationLong}

import io.circe.Json

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.databricks.MockEnvironment
import com.snowplowanalytics.snowplow.databricks.MockEnvironment.Action
import com.snowplowanalytics.snowplow.streams.TokenedEvents
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = sequential ^ s2"""
  The databricks loader should:
    Initialize the volume with an empty parquet file when there are no events $e0
    Insert events to Databricks and ack the events $e1
    Emit BadRows when there are badly formatted events $e2
    Write good batches and bad events when input contains both $e3
    Create unstruct_* column for unstructured events with valid schemas $e4
    Create contexts_* column for contexts with valid schemas $e5
    Create unstruct_* and contexts_* columns with valid schemas $e6
    Create recovery columns for unstructured events when schema evolution rules are broken $e7
    Create recovery columns for contexts when schema evolution rules are broken $e8
    Not create a unstruct_event column for a schema with no fields and additionalProperties false $e9
    Create a unstruct_event column for a schema with no fields and additionalProperties true $e10
    Not create a contexts column for a schema with no fields and additionalProperties false $e11
    Create a contexts column for a schema with no fields and additionalProperties true $e12
    Crash and exit for an unrecognized schema, if exitOnMissingIgluSchema is true $e13
    Crash and exit if events have schemas with clashing column names $e14
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

  def e4 = {
    val ueGood700 = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val ueGood701 = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 1)),
          Json.obj(
            "col_a" -> Json.fromString("xyz"),
            "col_b" -> Json.fromString("abc")
          )
        )
      )
    )

    val expectedUnstructColumns = Set(
      RowParquetRecord(
        "col_a" -> BinaryValue("xyz"),
        "col_b" -> NullValue
      ),
      RowParquetRecord(
        "col_a" -> BinaryValue("xyz"),
        "col_b" -> BinaryValue("abc")
      ),
      NullValue
    )

    val toInputs = for {
      inputs1 <- inputEvents(count = 1, good(ue = ueGood700))
      inputs2 <- inputEvents(count = 1, good(ue = ueGood701))
    } yield inputs1 ++ inputs2

    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(4), // 2 batches * 2 events each
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(4),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results.uploaded(1).flatMap(_.get("unstruct_event_myvendor_goodschema_7")).toSet should beEqualTo(expectedUnstructColumns)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e5 = {
    val cxGood700 = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val cxGood701 = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 1)),
          Json.obj(
            "col_a" -> Json.fromString("xyz"),
            "col_b" -> Json.fromString("abc")
          )
        )
      )
    )

    val expectedContextColumns = Set(
      ListParquetRecord(
        RowParquetRecord(
          "_schema_version" -> BinaryValue("7-0-0"),
          "col_a" -> BinaryValue("xyz"),
          "col_b" -> NullValue
        )
      ),
      ListParquetRecord(
        RowParquetRecord(
          "_schema_version" -> BinaryValue("7-0-1"),
          "col_a" -> BinaryValue("xyz"),
          "col_b" -> BinaryValue("abc")
        )
      ),
      NullValue
    )

    val toInputs = for {
      inputs1 <- inputEvents(count = 1, good(contexts = cxGood700))
      inputs2 <- inputEvents(count = 1, good(contexts = cxGood701))
    } yield inputs1 ++ inputs2

    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(4), // 2 batches * 2 events each
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(4),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results.uploaded(1).flatMap(_.get("contexts_myvendor_goodschema_7")).toSet should beEqualTo(expectedContextColumns)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e6 = {
    val ueGood700 = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val ueGood701 = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 1)),
          Json.obj(
            "col_a" -> Json.fromString("xyz"),
            "col_b" -> Json.fromString("abc")
          )
        )
      )
    )

    val cxGood700 = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val cxGood701 = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 1)),
          Json.obj(
            "col_a" -> Json.fromString("xyz"),
            "col_b" -> Json.fromString("abc")
          )
        )
      )
    )

    val expectedUnstructColumns = Set(
      RowParquetRecord(
        "col_a" -> BinaryValue("xyz"),
        "col_b" -> NullValue
      ),
      RowParquetRecord(
        "col_a" -> BinaryValue("xyz"),
        "col_b" -> BinaryValue("abc")
      ),
      NullValue
    )

    val expectedContextColumns = Set(
      ListParquetRecord(
        RowParquetRecord(
          "_schema_version" -> BinaryValue("7-0-0"),
          "col_a" -> BinaryValue("xyz"),
          "col_b" -> NullValue
        )
      ),
      ListParquetRecord(
        RowParquetRecord(
          "_schema_version" -> BinaryValue("7-0-1"),
          "col_a" -> BinaryValue("xyz"),
          "col_b" -> BinaryValue("abc")
        )
      ),
      NullValue
    )

    val toInputs = for {
      inputs1 <- inputEvents(count = 1, good(ue = ueGood700, contexts = cxGood700))
      inputs2 <- inputEvents(count = 1, good(ue = ueGood701, contexts = cxGood701))
    } yield inputs1 ++ inputs2

    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(4), // 2 batches * 2 events each
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(4),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results.uploaded(1).flatMap(_.get("unstruct_event_myvendor_goodschema_7")).toSet should beEqualTo(expectedUnstructColumns),
          results.uploaded(1).flatMap(_.get("contexts_myvendor_goodschema_7")).toSet should beEqualTo(expectedContextColumns)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e7 = {
    val ueBadEvolution100 = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "badevolution", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val ueBadEvolution101 = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "badevolution", "jsonschema", SchemaVer.Full(1, 0, 1)),
          Json.obj(
            "col_a" -> Json.fromInt(123)
          )
        )
      )
    )

    val expectedUnstructColumns: Set[(Value, Value)] = Set(
      (
        RowParquetRecord(
          "col_a" -> BinaryValue("xyz")
        ),
        NullValue
      ),
      (
        NullValue,
        RowParquetRecord(
          "col_a" -> LongValue(123)
        )
      ),
      (NullValue, NullValue)
    )

    val toInputs = for {
      inputs1 <- inputEvents(count = 1, good(ue = ueBadEvolution100))
      inputs2 <- inputEvents(count = 1, good(ue = ueBadEvolution101))
    } yield inputs1 ++ inputs2

    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(4), // 2 batches * 2 events each
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(4),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results
            .uploaded(1)
            .map { r =>
              (
                r.get("unstruct_event_myvendor_badevolution_1").get,
                r.get("unstruct_event_myvendor_badevolution_1_recovered_1_0_1_37fd804e").get
              )
            }
            .toSet should beEqualTo(expectedUnstructColumns)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e8 = {
    val cxBadEvolution100 = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "badevolution", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val cxBadEvolution101 = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "badevolution", "jsonschema", SchemaVer.Full(1, 0, 1)),
          Json.obj(
            "col_a" -> Json.fromInt(123)
          )
        )
      )
    )

    val expectedContextColumns: Set[(Value, Value)] = Set(
      (
        ListParquetRecord(RowParquetRecord("_schema_version" -> BinaryValue("1-0-0"), "col_a" -> BinaryValue("xyz"))),
        NullValue
      ),
      (
        NullValue,
        ListParquetRecord(RowParquetRecord("_schema_version" -> BinaryValue("1-0-1"), "col_a" -> LongValue(123)))
      ),
      (NullValue, NullValue)
    )

    val toInputs = for {
      inputs1 <- inputEvents(count = 1, good(contexts = cxBadEvolution100))
      inputs2 <- inputEvents(count = 1, good(contexts = cxBadEvolution101))
    } yield inputs1 ++ inputs2

    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(4), // 2 batches * 2 events each
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(4),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results
            .uploaded(1)
            .map { r =>
              (
                r.get("contexts_myvendor_badevolution_1").get,
                r.get("contexts_myvendor_badevolution_1_recovered_1_0_1_37fd804e").get
              )
            }
            .toSet should beEqualTo(expectedContextColumns)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e9 = {
    val ueNoFields = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "no-fields-v1", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj()
        )
      )
    )

    val io = runTest(inputEvents(count = 1, good(ue = ueNoFields))) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(2),
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(2),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results.uploaded(1).flatMap(_.iterator.map(_._1).toList) should not contain beMatching("unstruct_event_.*".r)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e10 = {
    val ueNoFields = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "no-fields-v2", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj()
        )
      )
    )

    val expectedUnstructColumns = Set(
      BinaryValue("{}"),
      NullValue
    )

    val io = runTest(inputEvents(count = 1, good(ue = ueNoFields))) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(2),
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(2),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results.uploaded(1).flatMap(_.get("unstruct_event_myvendor_no_fields_v2_1")).toSet should beEqualTo(expectedUnstructColumns)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e11 = {
    val cxNoFields = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "no-fields-v1", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj()
        )
      )
    )

    val io = runTest(inputEvents(count = 1, good(contexts = cxNoFields))) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(2),
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(2),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results.uploaded(1).flatMap(_.iterator.map(_._1).toList) should not contain beMatching("contexts_.*".r)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e12 = {
    val cxNoFields = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "no-fields-v2", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj()
        )
      )
    )

    val expectedContextColumns = Set(
      ListParquetRecord(BinaryValue("""{"_schema_version":"1-0-0"}""")),
      NullValue
    )

    val io = runTest(inputEvents(count = 1, good(contexts = cxNoFields))) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        results <- control.state.get
      } yield {
        val expectedActions = Vector(
          Action.UploadedFile,
          Action.AddedBadCountMetric(0),
          Action.UploadedFile,
          Action.AddedGoodCountMetric(2),
          Action.SetE2ELatencyMetric(Duration.Zero),
          Action.Checkpointed(List(inputs(0).ack))
        )
        List(
          results.actions should beEqualTo(expectedActions),
          results.uploaded(0) should beEmpty,
          results.uploaded(1) should haveLength(2),
          results.uploaded(1).map(_.get("event_id")) should contain(beSome[Value](not(beEqualTo(NullValue)))).forall,
          results.uploaded(1).flatMap(_.get("contexts_myvendor_no_fields_v2_1")).toSet should beEqualTo(expectedContextColumns)
        ).reduce(_ and _)
      }
    }

    TestControl.executeEmbed(io)
  }

  def e13 = {
    val ueDoesNotExist = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "doesnotexit", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val io = runTest(inputEvents(count = 1, good(ue = ueDoesNotExist))) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment.copy(exitOnMissingIgluSchema = true)).compile.drain
        _ <- control.state.get
      } yield ko
    }.handleError { e =>
      e.getMessage must contain("Exiting because failed to resolve Iglu schemas")
    }

    TestControl.executeEmbed(io)
  }

  def e14 = {
    val contextsWithClashingSchemas = Contexts(
      List(
        SelfDescribingData(
          SchemaKey("clashing.a", "b_c", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        ),
        SelfDescribingData(
          SchemaKey("clashing", "a_b_c", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val io = runTest(inputEvents(count = 1, good(contexts = contextsWithClashingSchemas))) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        _ <- control.state.get
      } yield ko
    }.handleError { e =>
      e.getMessage must contain("schemas [clashing.a.b_c, clashing.a_b_c] have clashing column names")
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
