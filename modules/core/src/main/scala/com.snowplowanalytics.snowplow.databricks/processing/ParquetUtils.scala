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

import cats.Foldable
import cats.data.NonEmptyVector
import cats.implicits._
import cats.effect.Sync
import io.circe.Json
import fs2.Stream
import com.github.mjakubowski84.parquet4s._
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import com.github.mjakubowski84.parquet4s.{ParquetWriter, Path, RowParquetRecord}
import com.github.mjakubowski84.parquet4s.parquet.writeSingleFile
import org.apache.hadoop.conf.Configuration

import java.time.{Instant, LocalDate}
import java.nio.ByteBuffer

import com.snowplowanalytics.iglu.schemaddl.parquet.{Caster, Type}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadProcessor}
import com.snowplowanalytics.snowplow.loaders.transform.{NonAtomicFields, Transform}
import com.snowplowanalytics.snowplow.sinks.ListOfList
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._

private[processing] object ParquetUtils {

  def write[F[_]: Sync](
    hadoopConf: Configuration,
    compression: CompressionCodecName,
    schema: MessageType,
    events: List[RowParquetRecord]
  ): Stream[F, Nothing] =
    writeSingleFile[F]
      .generic(schema)
      .options(ParquetWriter.Options(hadoopConf = hadoopConf, compressionCodecName = compression))
      .write(Path("/output.parquet")) // file name is not important
      .apply(Stream.emits(events))

  case class TransformResult(bad: List[BadRow], good: List[RowParquetRecord])

  def transform[F[_]: Sync](
    badProcessor: BadProcessor,
    events: ListOfList[Event],
    entities: NonAtomicFields.Result
  ): F[TransformResult] =
    Foldable[ListOfList]
      .traverseSeparateUnordered(events) { event =>
        Sync[F].delay {
          Transform
            .transformEvent(badProcessor, caster, event, entities)
            .map(rowParquetRecord(_))
        }
      }
      .map { case (bad, good) =>
        TransformResult(bad, good)
      }

  private val caster: Caster[Value] = new Caster[Value] {
    def nullValue: Value                = NullValue
    def jsonValue(v: Json): Value       = BinaryValue(v.noSpaces)
    def stringValue(v: String): Value   = BinaryValue(v)
    def booleanValue(v: Boolean): Value = BooleanValue(v)
    def intValue(v: Int): Value         = IntValue(v)
    def longValue(v: Long): Value       = LongValue(v)
    def doubleValue(v: Double): Value   = DoubleValue(v)
    def decimalValue(unscaled: BigInt, details: Type.Decimal): Value =
      details.precision match {
        case Type.DecimalPrecision.Digits9 =>
          IntValue(unscaled.intValue)
        case Type.DecimalPrecision.Digits18 =>
          LongValue(unscaled.underlying.longValueExact)
        case Type.DecimalPrecision.Digits38 =>
          encodeDecimalAsByteArray(unscaled)
      }
    def timestampValue(v: Instant): Value = LongValue(v.toEpochMilli * 1000L)
    def dateValue(v: LocalDate): Value    = IntValue(v.toEpochDay.toInt)
    def arrayValue(vs: Vector[Value]): Value =
      ListParquetRecord(vs: _*)
    def structValue(vs: NonEmptyVector[Caster.NamedValue[Value]]): Value =
      rowParquetRecord(vs.toVector)
  }

  private def rowParquetRecord(vs: Vector[Caster.NamedValue[Value]]): RowParquetRecord = {
    val elements = vs.map(v => v.name -> v.value)
    RowParquetRecord(elements: _*)
  }

  private def encodeDecimalAsByteArray(unscaled: BigInt): BinaryValue = {
    val unscaledBytes   = unscaled.toByteArray
    val bytesDifference = ParquetSchema.byteArrayLengthForDecimal - unscaledBytes.length
    if (bytesDifference === 0)
      BinaryValue(unscaledBytes)
    else {
      val buffer     = ByteBuffer.allocate(ParquetSchema.byteArrayLengthForDecimal)
      val sign: Byte = if (unscaledBytes.head < 0) -1 else 0
      // sign as head, unscaled as tail of buffer
      (0 until bytesDifference).foreach(_ => buffer.put(sign))
      buffer.put(unscaledBytes)
      BinaryValue(Binary.fromReusedByteArray(buffer.array()))
    }
  }

}
