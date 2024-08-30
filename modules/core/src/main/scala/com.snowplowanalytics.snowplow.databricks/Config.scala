/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.databricks

import cats.Id
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import com.comcast.ip4s.Port
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, Metrics => CommonMetrics, Retrying, Telemetry, Webhook}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs.schemaCriterionDecoder
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._

case class Config[+Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  batching: Config.Batching,
  retries: Config.Retries,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring,
  license: AcceptedLicense,
  skipSchemas: List[SchemaCriterion],
  legacyColumns: List[SchemaCriterion],
  exitOnMissingIgluSchema: Boolean
)

object Config {

  case class WithIglu[+Source, +Sink](main: Config[Source, Sink], iglu: ResolverConfig)

  case class Output[+Sink](good: Databricks, bad: SinkWithMaxSize[Sink])

  case class SinkWithMaxSize[+Sink](sink: Sink, maxRecordSize: Int)

  case class MaxRecordSize(maxRecordSize: Int)

  case class Databricks(
    host: String,
    token: String,
    catalog: String,
    schema: String,
    volume: String,
    compression: CompressionCodecName
  )

  case class Batching(
    maxBytes: Int,
    maxDelay: FiniteDuration,
    uploadConcurrency: Int
  )

  case class Retries(
    setupErrors: Retrying.Config.ForSetup,
    transientErrors: Retrying.Config.ForTransient
  )

  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig]
  )

  case class SentryM[M[_]](
    dsn: M[String],
    tags: Map[String, String]
  )

  type Sentry = SentryM[Id]

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry],
    healthProbe: HealthProbe,
    webhook: Webhook.Config
  )

  implicit def decoder[Source: Decoder, Sink: Decoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val compressionDecoder = Decoder.decodeString.emapTry { str =>
      Try(CompressionCodecName.valueOf(str.toUpperCase))
    }
    implicit val sinkWithMaxSize = for {
      sink <- Decoder[Sink]
      maxSize <- deriveConfiguredDecoder[MaxRecordSize]
    } yield SinkWithMaxSize(sink, maxSize.maxRecordSize)
    implicit val databricks = deriveConfiguredDecoder[Databricks]
    implicit val output     = deriveConfiguredDecoder[Output[Sink]]
    implicit val batching   = deriveConfiguredDecoder[Batching]
    implicit val sentryDecoder = deriveConfiguredDecoder[SentryM[Option]]
      .map[Option[Sentry]] {
        case SentryM(Some(dsn), tags) =>
          Some(SentryM[Id](dsn, tags))
        case SentryM(None, _) =>
          None
      }
    implicit val metricsDecoder     = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]
    implicit val retriesDecoder     = deriveConfiguredDecoder[Retries]

    // TODO add databricks docs
    implicit val licenseDecoder =
      AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.0/"))

    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
