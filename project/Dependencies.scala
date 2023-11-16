/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
import sbt._

object Dependencies {

  object V {
    // Scala
    val catsEffect       = "3.5.0"
    val catsRetry        = "3.1.0"
    val http4s           = "0.23.15"
    val decline          = "2.4.1"
    val circe            = "0.14.3"
    val betterMonadicFor = "0.3.1"
    val parquet4s        = "2.13.0"

    // java
    val slf4j    = "2.0.7"
    val azureSdk = "1.9.1"
    val sentry   = "6.25.2"
    val jaxb     = "2.3.1"
    val awsSdk2  = "2.20.135"
    val hadoop   = "3.3.6"

    // Snowplow
    val streams    = "0.1.0"
    val igluClient = "3.1.0"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

  }

  val catsRetry         = "com.github.cb372"         %% "cats-retry"           % V.catsRetry
  val blazeClient       = "org.http4s"               %% "http4s-blaze-client"  % V.http4s
  val decline           = "com.monovore"             %% "decline-effect"       % V.decline
  val circeGenericExtra = "io.circe"                 %% "circe-generic-extras" % V.circe
  val betterMonadicFor  = "com.olegpy"               %% "better-monadic-for"   % V.betterMonadicFor
  val parquet4s         = "com.github.mjakubowski84" %% "parquet4s-fs2"        % V.parquet4s

  // java
  val slf4j         = "org.slf4j"              % "slf4j-simple"   % V.slf4j
  val azureIdentity = "com.azure"              % "azure-identity" % V.azureSdk
  val sentry        = "io.sentry"              % "sentry"         % V.sentry
  val jaxb          = "javax.xml.bind"         % "jaxb-api"       % V.jaxb
  val stsSdk2       = "software.amazon.awssdk" % "sts"            % V.awsSdk2
  val hadoopClient = ("org.apache.hadoop" % "hadoop-client" % V.hadoop)
    .exclude("com.jcraft", "jsch")
    .exclude("org.apache.zookeeper", "zookeeper")

  val streamsCore      = "com.snowplowanalytics" %% "streams-core"             % V.streams
  val kinesis          = "com.snowplowanalytics" %% "kinesis"                  % V.streams
  val kafka            = "com.snowplowanalytics" %% "kafka"                    % V.streams
  val pubsub           = "com.snowplowanalytics" %% "pubsub"                   % V.streams
  val loaders          = "com.snowplowanalytics" %% "loaders-common"           % V.streams
  val runtime          = "com.snowplowanalytics" %% "runtime-common"           % V.streams
  val igluClientHttp4s = "com.snowplowanalytics" %% "iglu-scala-client-http4s" % V.igluClient

  // tests
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2           % Test
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % V.catsEffectSpecs2 % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect       % Test

  val coreDependencies = Seq(
    streamsCore,
    loaders,
    runtime,
    igluClientHttp4s,
    hadoopClient,
    parquet4s,
    catsRetry,
    blazeClient,
    decline,
    sentry,
    circeGenericExtra,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4j % Test
  )

  val kafkaDependencies = Seq(
    kafka,
    slf4j % Runtime,
    jaxb  % Runtime,
    azureIdentity
  )

  val pubsubDependencies = Seq(
    pubsub,
    jaxb  % Runtime,
    slf4j % Runtime
  )

  val kinesisDependencies = Seq(
    kinesis,
    jaxb    % Runtime,
    slf4j   % Runtime,
    stsSdk2 % Runtime
  )

}
