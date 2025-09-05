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
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, HttpClient, Metrics => CommonMetrics, Retrying, Telemetry, Webhook}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs.schemaCriterionDecoder
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._

case class Config[+Factory, +Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  streams: Factory,
  batching: Config.Batching,
  cpuParallelismFactor: BigDecimal,
  retries: Config.Retries,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring,
  license: AcceptedLicense,
  skipSchemas: List[SchemaCriterion],
  exitOnMissingIgluSchema: Boolean,
  http: Config.Http,
  dev: Config.DevFeatures
)

object Config {

  case class WithIglu[+Factory, +Source, +Sink](main: Config[Factory, Source, Sink], iglu: ResolverConfig)

  case class Output[+Sink](good: Databricks, bad: SinkWithMaxSize[Sink])

  case class SinkWithMaxSize[+Sink](sink: Sink, maxRecordSize: Int)

  case class MaxRecordSize(maxRecordSize: Int)

  case class Databricks(
    host: String,
    token: Option[String],
    oauth: Option[DatabricksOAuth],
    catalog: String,
    schema: String,
    volume: String,
    compression: CompressionCodecName
  )

  case class DatabricksOAuth(clientId: String, clientSecret: String)

  case class Batching(
    maxBytes: Int,
    maxDelay: FiniteDuration,
    uploadParallelismFactor: BigDecimal
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

  case class Http(client: HttpClient.Config)

  /**
   * Features that are never intended for production pipelines. Only exist to help the maintainers
   * of this loader.
   *
   * @param setEtlTstamp
   *   Overrides the event's `etl_tstamp` field, replacing it with the time the parquet file is
   *   pushed to the Databricks volume. Helpful to devs because we can measure the difference
   *   between `etl_tstamp` (set by this loader) and `load_tstamp` (set by Databricks)
   */
  case class DevFeatures(setEtlTstamp: Boolean)

  implicit def decoder[Factory: Decoder, Source: Decoder, Sink: Decoder]: Decoder[Config[Factory, Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val compressionDecoder = Decoder.decodeString.emapTry { str =>
      Try(CompressionCodecName.valueOf(str.toUpperCase))
    }
    implicit val sinkWithMaxSize = for {
      sink <- Decoder[Sink]
      maxSize <- deriveConfiguredDecoder[MaxRecordSize]
    } yield SinkWithMaxSize(sink, maxSize.maxRecordSize)
    implicit val oauth = deriveConfiguredDecoder[DatabricksOAuth]
    implicit val databricks =
      deriveConfiguredDecoder[Databricks].ensure(c => c.token.isDefined || c.oauth.isDefined, "Missing either .token or .oauth")
    implicit val output   = deriveConfiguredDecoder[Output[Sink]]
    implicit val batching = deriveConfiguredDecoder[Batching]
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
    implicit val httpDecoder        = deriveConfiguredDecoder[Http]
    implicit val devFeaturesDecoder = deriveConfiguredDecoder[DevFeatures]

    implicit val licenseDecoder =
      AcceptedLicense.decoder(
        AcceptedLicense.DocumentationLink(
          "https://docs.snowplow.io/docs/api-reference/loaders-storage-targets/databricks-streaming-loader/configuration-reference/#license"
        )
      )

    deriveConfiguredDecoder[Config[Factory, Source, Sink]]
  }

}
