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

import cats.implicits._
import cats.effect.{Async, Sync}
import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.{DatabricksConfig, UserAgent}
import com.databricks.sdk.core.error.platform.{NotFound, PermissionDenied, Unauthenticated}
import com.databricks.sdk.service.files.{FilesAPI, UploadRequest}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.ByteArrayInputStream
import java.util.UUID
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.net.UnknownHostException

import com.snowplowanalytics.snowplow.databricks.{Alert, Config, RuntimeService}
import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}

trait DatabricksUploader[F[_]] {
  def upload(bytes: ByteArrayInputStream, filename: String): F[Unit]
}

object DatabricksUploader {

  trait WithHandledErrors[F[_]] {
    def upload(bytes: ByteArrayInputStream): F[Unit]
  }

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Sync](config: Config.Databricks): F[DatabricksUploader[F]] =
    for {
      _ <- Sync[F].delay(UserAgent.withProduct("snowplow-loader", "0.0.0"))
      ws <- Sync[F].delay(new WorkspaceClient(databricksConfig(config)))
    } yield impl(ws.files)

  def withHandledErrors[F[_]: Async](
    underlying: DatabricksUploader[F],
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    config: Config.Databricks,
    retries: Config.Retries
  ): WithHandledErrors[F] = new WithHandledErrors[F] {
    def upload(bytes: ByteArrayInputStream): F[Unit] =
      generatePath(config).flatMap { path =>
        Retrying.withRetries(
          appHealth,
          retries.transientErrors,
          retries.setupErrors,
          RuntimeService.DatabricksUploader,
          Alert.FailedToUploadFile,
          isSetupError
        ) {
          // Reset first, in case this is a retry
          Sync[F].delay(bytes.reset()) >>
            underlying.upload(bytes, path)
        } <* appHealth.beHealthyForSetup
      }
  }

  private def impl[F[_]: Sync](api: FilesAPI): DatabricksUploader[F] = new DatabricksUploader[F] {
    def upload(bytes: ByteArrayInputStream, path: String): F[Unit] =
      for {
        _ <- Logger[F].debug(show"Uploading file of size ${bytes.available} to $path")
        req = new UploadRequest().setFilePath(path).setContents(bytes).setOverwrite(false)
        _ <- Sync[F].blocking(api.upload(req))
      } yield ()
  }

  private def generatePath[F[_]: Sync](config: Config.Databricks): F[String] =
    for {
      uuid <- Sync[F].delay(UUID.randomUUID)
      now <- Sync[F].realTimeInstant
      partition = timePartition(now)
      name      = filename(config, now, uuid)
    } yield s"/Volumes/${config.catalog}/${config.schema}/${config.volume}/events/$partition/$name"

  private def databricksConfig(config: Config.Databricks): DatabricksConfig = {
    val c = new DatabricksConfig()
      .setHost(config.host)
    config.token.foreach(c.setToken(_))
    config.oauth.foreach { oauth =>
      c.setClientId(oauth.clientId)
      c.setClientSecret(oauth.clientSecret)
    }
    c
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
    s"$prefix-$uuid$ext.parquet"
  }

  private def timePartition(loadTstamp: Instant): String = {
    val value = dayFormatter.format(loadTstamp)
    s"load_tstamp_date=$value"
  }

  def isSetupError: PartialFunction[Throwable, String] = {
    case _: Unauthenticated =>
      "Unauthenticated: Invalid connection details"
    case pd: PermissionDenied =>
      // PermissionDenied exception messages are clean, short and helpful
      s"Permission denied by Databricks: ${pd.getMessage}"
    case nf: NotFound =>
      if (nf.getErrorCode == "FOUND") {
        // If hostname returns 404, it returns NotFound exception
        // with "FOUND" error code.
        // We return static message because error message from exception
        // contains response body from the hostname, and it might be verbose
        // html source code as well.
        "Invalid Databricks hostname"
      } else {
        // It returns NotFound exception for non-existing resources
        // as well. However, error code is "NOT_FOUND" in those cases.
        // NotFound exception messages are clean, short and helpful
        // in those cases.
        s"Not Found: ${nf.getMessage}"
      }
    case _: UnknownHostException =>
      "Invalid Databricks hostname"
  }

}
