/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.databricks.processing

import cats.implicits._
import cats.effect.Sync
import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.{DatabricksConfig, UserAgent}
import com.databricks.sdk.service.files.FilesAPI
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.ByteArrayInputStream
import java.util.UUID
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.snowplowanalytics.snowplow.databricks.Config

trait DatabricksUploader[F[_]] {
  def upload(bytes: ByteArrayInputStream): F[Unit]
}

object DatabricksUploader {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Sync](config: Config.Databricks): F[DatabricksUploader[F]] =
    for {
      _ <- Sync[F].delay(UserAgent.withProduct("snowplow-loader", "0.0.0"))
      ws <- Sync[F].delay(new WorkspaceClient(databricksConfig(config)))
    } yield impl(config, ws.files)

  def impl[F[_]: Sync](config: Config.Databricks, api: FilesAPI): DatabricksUploader[F] = new DatabricksUploader[F] {
    def upload(bytes: ByteArrayInputStream): F[Unit] =
      for {
        uuid <- Sync[F].delay(UUID.randomUUID)
        now <- Sync[F].realTimeInstant
        partition = timePartition(now)
        name      = filename(config, now, uuid)
        path      = s"Volumes/${config.catalog}/${config.schema}/${config.volume}/events/$partition/$name"
        _ <- Logger[F].info(show"Uploading file of size ${bytes.available} to $path")
        _ <- Sync[F].blocking(api.upload(path, bytes))
      } yield ()
  }

  private def databricksConfig(config: Config.Databricks): DatabricksConfig =
    new DatabricksConfig()
      .setHost(config.host)
      .setToken(config.token)

  private val dayFormatter: DateTimeFormatter    = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC)
  private val secondFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneOffset.UTC)

  private def filename(
    config: Config.Databricks,
    loadTstamp: Instant,
    uuid: UUID
  ): String = {
    val ext    = config.compression.getExtension
    val prefix = secondFormatter.format(loadTstamp)
    s"$prefix-$uuid$ext.parquet"
  }

  private def timePartition(loadTstamp: Instant): String = {
    val value = dayFormatter.format(loadTstamp)
    s"load_tstamp_date=$value"
  }

}
