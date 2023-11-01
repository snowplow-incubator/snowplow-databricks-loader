/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.databricks.processing

import cats.implicits._
import cats.ApplicativeThrow
import cats.effect.Sync
import cats.effect.std.UUIDGen
import fs2.{Chunk, Stream}
import org.http4s.client.Client
import org.http4s.{AuthScheme, Credentials, Headers, MediaType, Method, Request, Status}
import org.http4s.headers.{Authorization, `Content-Type`}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.ByteBuffer
import java.util.UUID
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.snowplowanalytics.snowplow.databricks.Config

trait DatabricksUploader[F[_]] {
  def upload(bytes: ByteBuffer, loadTsamp: Instant): F[Unit]
}

object DatabricksUploader {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def impl[F[_]: Sync](config: Config.Databricks, client: Client[F]): DatabricksUploader[F] = new DatabricksUploader[F] {
    def upload(bytes: ByteBuffer, loadTstamp: Instant): F[Unit] =
      for {
        uuid <- UUIDGen[F].randomUUID
        partition = timePartition(loadTstamp)
        name      = filename(config, loadTstamp, uuid)
        uri       = config.host / "api" / "2.0" / "fs" / "Volumes" / config.catalog / config.schema / config.volume / partition / name
        req = Request[F](
                method  = Method.PUT,
                uri     = uri,
                headers = headers(config),
                body    = Stream.chunk(Chunk.byteBuffer(bytes))
              )
        _ <- Logger[F].info(show"Uploading file of size ${bytes.limit} to $uri")
        status <- client.status(req)
        _ <- raiseForStatus[F](status)
      } yield ()
  }

  private val dayFormatter: DateTimeFormatter    = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC)
  private val secondFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneOffset.UTC)

  private def filename(
    config: Config.Databricks,
    loadTstamp: Instant,
    uuid: UUID
  ): String = {
    val ext    = config.compression.getExtension
    val prefix = secondFormatter.format(loadTstamp)
    s"$prefix-$uuid.$ext.parquet"
  }

  private def timePartition(loadTstamp: Instant): String = {
    val value = dayFormatter.format(loadTstamp)
    s"load_tstamp_date=$value"
  }

  private def headers(config: Config.Databricks): Headers =
    Headers(
      Authorization(Credentials.Token(AuthScheme.Bearer, config.token)),
      `Content-Type`(MediaType.application.`octet-stream`)
    )

  private def raiseForStatus[F[_]: ApplicativeThrow](status: Status): F[Unit] =
    if (status.isSuccess)
      ApplicativeThrow[F].unit
    else
      ApplicativeThrow[F].raiseError(new RuntimeException(s"Unexpected status from Databricks upload: $status"))
}
