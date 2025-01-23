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
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageType

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.databricks.Config
import com.snowplowanalytics.snowplow.sinks.ListOfList
import com.snowplowanalytics.snowplow.loaders.transform.NonAtomicFields

import scala.concurrent.duration.Duration
import java.util.UUID
import java.time.Instant

class ParquetSerializerSpec extends Specification with CatsEffect {
  import ParquetSerializerSpec._

  def is = s2"""
  The parquet serializer should
    Serialize an empty parquet file $e1
    Serialize an empty parquet file which is the same size each time $e2
    Serialize a non-empty parquet file which is larger than the empty parquet file $e3
  """

  def e1 = ParquetSerializer.resource[IO](testConfig, CompressionCodecName.SNAPPY).use { serializer =>
    for {
      byteStream <- serializer.serialize(testSchema, Nil)
    } yield byteStream.available() must beGreaterThan(1000)
  }

  def e2 = ParquetSerializer.resource[IO](testConfig, CompressionCodecName.SNAPPY).use { serializer =>
    for {
      byteStream1 <- serializer.serialize(testSchema, Nil)
      byteStream2 <- serializer.serialize(testSchema, Nil)
    } yield byteStream1.available() must beEqualTo(byteStream2.available())
  }

  def e3 = ParquetSerializer.resource[IO](testConfig, CompressionCodecName.SNAPPY).use { serializer =>
    val event = Event.minimal(UUID.randomUUID(), Instant.now(), "0.1.0", "0.1.0")

    for {
      TransformUtils.TransformResult(_, events) <-
        TransformUtils.transform[IO](testBadProcessor, ListOfList.ofItems(event), NonAtomicFields.Result(Vector.empty, List.empty))
      byteStream1 <- serializer.serialize(testSchema, Nil)
      byteStream2 <- serializer.serialize(testSchema, events)
    } yield byteStream1.available() must beLessThan(byteStream2.available())
  }

}

object ParquetSerializerSpec {

  val testConfig: Config.Batching = Config.Batching(10, Duration.Zero, 1)

  val testSchema: MessageType = ParquetSchema.forBatch(Vector.empty)

  val testBadProcessor = BadRowProcessor("test", "test")
}
