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

import cats.effect.{Resource, Sync}
import cats.implicits._
import fs2.Stream
import com.github.mjakubowski84.parquet4s.{ParquetWriter, Path, RowParquetRecord}
import com.github.mjakubowski84.parquet4s.parquet.writeSingleFile
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageType

import com.snowplowanalytics.snowplow.databricks.Config

import java.io.ByteArrayInputStream

class ParquetSerializer[F[_]: Sync] private (configuredFS: InMemoryFileSystem.Configured[F], compression: CompressionCodecName) {

  /**
   * Serializes the events to parquet format, creating an in-memory byte stream
   *
   * It is UNSAFE for multiple threads/fibers to call this method at the same time.
   *
   * The calling thread/fiber is responsible for making sure it is accessed sequentially.
   */
  def serialize(schema: MessageType, events: List[RowParquetRecord]): F[ByteArrayInputStream] =
    writeToInMemoryFS(schema, events).compile.drain >> configuredFS.getBytes

  private def writeToInMemoryFS(
    schema: MessageType,
    events: List[RowParquetRecord]
  ): Stream[F, Nothing] =
    writeSingleFile[F]
      .generic(schema)
      .options(ParquetWriter.Options(hadoopConf = configuredFS.hadoopConf, compressionCodecName = compression))
      .write(Path("/output.parquet")) // file name is not important
      .apply(Stream.emits(events))
}

object ParquetSerializer {

  def resource[F[_]: Sync](config: Config.Batching, compression: CompressionCodecName): Resource[F, ParquetSerializer[F]] =
    InMemoryFileSystem.configure(config).map { configuredFS =>
      new ParquetSerializer[F](configuredFS, compression)
    }

}
