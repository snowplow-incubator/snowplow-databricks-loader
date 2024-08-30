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
    val catsEffect       = "3.5.4"
    val http4s           = "0.23.16"
    val decline          = "2.4.1"
    val circe            = "0.14.4"
    val betterMonadicFor = "0.3.1"
    val parquet4s        = "2.19.0"

    // java
    val databricks = "0.30.0"
    val slf4j      = "2.0.16"
    val azureSdk   = "1.13.2"
    val sentry     = "7.14.0"
    val jaxb       = "2.3.1"
    val awsSdk2    = "2.27.16"
    val hadoop     = "3.4.0"

    // Snowplow
    val streams    = "0.8.0-M2"
    val igluClient = "3.1.1"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

  }

  val blazeClient       = "org.http4s"   %% "http4s-blaze-client"  % V.http4s
  val decline           = "com.monovore" %% "decline-effect"       % V.decline
  val circeGenericExtra = "io.circe"     %% "circe-generic-extras" % V.circe
  val betterMonadicFor  = "com.olegpy"   %% "better-monadic-for"   % V.betterMonadicFor
  val parquet4s = ("com.github.mjakubowski84" %% "parquet4s-fs2" % V.parquet4s)
    .exclude("com.github.luben", "zstd-jni")

  // java
  val databricks    = "com.databricks"         % "databricks-sdk-java" % V.databricks
  val slf4j         = "org.slf4j"              % "slf4j-simple"        % V.slf4j
  val azureIdentity = "com.azure"              % "azure-identity"      % V.azureSdk
  val sentry        = "io.sentry"              % "sentry"              % V.sentry
  val jaxb          = "javax.xml.bind"         % "jaxb-api"            % V.jaxb
  val stsSdk2       = "software.amazon.awssdk" % "sts"                 % V.awsSdk2
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
    databricks,
    hadoopClient,
    parquet4s,
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
