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

import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import org.http4s.client.Client
import io.sentry.Sentry

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.databricks.processing.{DatabricksUploader, ParquetSerializer}
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe, HttpClient, Webhook}

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  resolver: Resolver[F],
  httpClient: Client[F],
  databricks: DatabricksUploader.WithHandledErrors[F],
  serializer: ParquetSerializer[F],
  metrics: Metrics[F],
  appHealth: AppHealth.Interface[F, Alert, RuntimeService],
  batching: Config.Batching,
  badRowMaxSize: Int,
  schemasToSkip: List[SchemaCriterion],
  exitOnMissingIgluSchema: Boolean,
  devFeatures: Config.DevFeatures
)

object Environment {

  def fromConfig[F[_]: Async, FactoryConfig, SourceConfig, SinkConfig](
    config: Config.WithIglu[FactoryConfig, SourceConfig, SinkConfig],
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, SinkConfig]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- enableSentry[F](appInfo, config.main.monitoring.sentry)
      factory <- toFactory(config.main.streams)
      sourceAndAck <- factory.source(config.main.input)
      sourceReporter = sourceAndAck.isHealthy(config.main.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, Alert, RuntimeService](List(sourceReporter)))
      resolver <- mkResolver[F](config.iglu)
      httpClient <- HttpClient.resource[F](config.main.http.client)
      _ <- HealthProbe.resource(config.main.monitoring.healthProbe.port, appHealth)
      _ <- Webhook.resource(config.main.monitoring.webhook, appInfo, httpClient, appHealth)
      badSink <- factory
                   .sink(config.main.output.bad.sink)
                   .onError(_ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)))
      metrics <- Resource.eval(Metrics.build(config.main.monitoring.metrics, sourceAndAck))
      databricks <- Resource.eval(DatabricksUploader.build[F](config.main.output.good))
      databricksWrapped = DatabricksUploader.withHandledErrors(databricks, appHealth, config.main.output.good, config.main.retries)
      serializer <- ParquetSerializer.resource(config.main.batching, config.main.output.good.compression)
    } yield Environment(
      appInfo                 = appInfo,
      source                  = sourceAndAck,
      badSink                 = badSink,
      resolver                = resolver,
      httpClient              = httpClient,
      databricks              = databricksWrapped,
      serializer              = serializer,
      metrics                 = metrics,
      appHealth               = appHealth,
      batching                = config.main.batching,
      badRowMaxSize           = config.main.output.bad.maxRecordSize,
      schemasToSkip           = config.main.skipSchemas,
      exitOnMissingIgluSchema = config.main.exitOnMissingIgluSchema,
      devFeatures             = config.main.dev
    )

  private def enableSentry[F[_]: Sync](appInfo: AppInfo, config: Option[Config.Sentry]): Resource[F, Unit] =
    config match {
      case Some(c) =>
        val acquire = Sync[F].delay {
          Sentry.init { options =>
            options.setDsn(c.dsn)
            options.setRelease(appInfo.version)
            c.tags.foreach { case (k, v) =>
              options.setTag(k, v)
            }
          }
        }

        Resource.makeCase(acquire) {
          case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(Sentry.captureException(e)).void
          case _                                 => Sync[F].unit
        }
      case None =>
        Resource.unit[F]
    }

  private def mkResolver[F[_]: Async](resolverConfig: Resolver.ResolverConfig): Resource[F, Resolver[F]] =
    Resource.eval {
      Resolver
        .fromConfig[F](resolverConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu resolver config", e))
        .value
        .rethrow
    }

}
