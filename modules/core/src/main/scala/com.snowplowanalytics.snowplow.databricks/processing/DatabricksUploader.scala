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

import com.snowplowanalytics.snowplow.databricks.Config

trait DatabricksUploader[F[_]] {
  def upload(bytes: ByteBuffer): F[Unit]
}

object DatabricksUploader {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  // TODO: compression suffix?
  // TODO: partitioning?
  def impl[F[_]: Sync](config: Config.Databricks, client: Client[F]): DatabricksUploader[F] = new DatabricksUploader[F] {
    def upload(bytes: ByteBuffer): F[Unit] =
      for {
        uuid <- UUIDGen[F].randomUUID
        uri = config.host / "api" / "2.0" / "fs" / s"$uuid.parquet"
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
