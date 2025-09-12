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
import sbt._

object Dependencies {

  object V {
    // Scala
    val catsEffect       = "3.5.4"
    val decline          = "2.4.1"
    val circe            = "0.14.4"
    val betterMonadicFor = "0.3.1"
    val parquet4s        = "2.19.0"

    // java
    val databricks       = "0.62.0"
    val slf4j            = "2.0.16"
    val azureSdk         = "1.13.2"
    val sentry           = "7.14.0"
    val jaxb             = "2.3.1"
    val awsSdk2          = "2.27.16"
    val hadoop           = "3.4.2"
    val avro             = "1.11.4" // Version override
    val commonsBeanutils = "1.11.0" // Version override
    val dnsJava          = "3.6.0" // Version override
    val netty            = "4.1.125.Final" // Version override
    val commonsCompress  = "1.26.0" // Version override
    val commonsLang3     = "3.18.0" // Version override
    val nimbusJoseJwt    = "9.37.2" // Version override
    val kafkaClients     = "3.9.1" // Version override
    val jsonSmart        = "2.5.2" // Version override
    val protobufJava     = "3.25.5" // Version override

    // Snowplow
    val streams    = "0.13.1"
    val igluClient = "4.0.0"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

  }

  val decline           = "com.monovore" %% "decline-effect"       % V.decline
  val circeGenericExtra = "io.circe"     %% "circe-generic-extras" % V.circe
  val betterMonadicFor  = "com.olegpy"   %% "better-monadic-for"   % V.betterMonadicFor
  val parquet4s = ("com.github.mjakubowski84" %% "parquet4s-fs2" % V.parquet4s)
    .exclude("com.github.luben", "zstd-jni")

  // java
  val databricks     = "com.databricks"         % "databricks-sdk-java" % V.databricks
  val slf4j          = "org.slf4j"              % "slf4j-simple"        % V.slf4j
  val log4jOverSlf4j = "org.slf4j"              % "log4j-over-slf4j"    % V.slf4j
  val azureIdentity  = "com.azure"              % "azure-identity"      % V.azureSdk
  val sentry         = "io.sentry"              % "sentry"              % V.sentry
  val jaxb           = "javax.xml.bind"         % "jaxb-api"            % V.jaxb
  val stsSdk2        = "software.amazon.awssdk" % "sts"                 % V.awsSdk2
  val hadoopClient = ("org.apache.hadoop" % "hadoop-client" % V.hadoop)
    .exclude("com.jcraft", "jsch")
    .exclude("org.apache.zookeeper", "zookeeper")
  val avro             = "org.apache.avro"     % "avro"              % V.avro
  val commonsBeanutils = "commons-beanutils"   % "commons-beanutils" % V.commonsBeanutils
  val dnsJava          = "dnsjava"             % "dnsjava"           % V.dnsJava
  val nettyCodecHttp   = "io.netty"            % "netty-codec-http"  % V.netty
  val nettyCodecHttp2  = "io.netty"            % "netty-codec-http2" % V.netty
  val nettyHandler     = "io.netty"            % "netty-handler"     % V.netty
  val commonsCompress  = "org.apache.commons"  % "commons-compress"  % V.commonsCompress
  val commonsLang3     = "org.apache.commons"  % "commons-lang3"     % V.commonsLang3
  val nimbusJoseJwt    = "com.nimbusds"        % "nimbus-jose-jwt"   % V.nimbusJoseJwt
  val kafkaClients     = "org.apache.kafka"    % "kafka-clients"     % V.kafkaClients
  val jsonSmart        = "net.minidev"         % "json-smart"        % V.jsonSmart
  val protobufJava     = "com.google.protobuf" % "protobuf-java"     % V.protobufJava

  val streamsCore      = "com.snowplowanalytics" %% "streams-core"             % V.streams
  val kinesis          = "com.snowplowanalytics" %% "kinesis"                  % V.streams
  val kafka            = "com.snowplowanalytics" %% "kafka"                    % V.streams
  val pubsub           = "com.snowplowanalytics" %% "pubsub"                   % V.streams
  val loaders          = "com.snowplowanalytics" %% "loaders-common"           % V.streams
  val runtime          = "com.snowplowanalytics" %% "runtime-common"           % V.streams
  val igluClientHttp4s = "com.snowplowanalytics" %% "iglu-scala-client-http4s" % V.igluClient

  // tests
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % V.catsEffectSpecs2
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect

  val coreDependencies = Seq(
    streamsCore,
    loaders,
    runtime,
    igluClientHttp4s,
    databricks,
    hadoopClient,
    parquet4s,
    decline,
    sentry,
    circeGenericExtra,
    avro, // for security vulnerabilities
    commonsBeanutils, // for security vulnerabilities
    dnsJava, // for security vulnerabilities
    nettyCodecHttp, // for security vulnerabilities
    nettyCodecHttp2, // for security vulnerabilities
    nettyHandler, // for security vulnerabilities
    commonsCompress, // for security vulnerabilities
    commonsLang3, // for security vulnerabilities
    nimbusJoseJwt, // for security vulnerabilities
    specs2            % Test,
    catsEffectSpecs2  % Test,
    catsEffectTestkit % Test,
    slf4j             % Test,
    log4jOverSlf4j    % Test
  )

  val kafkaDependencies = Seq(
    kafka,
    azureIdentity,
    kafkaClients, // for security vulnerabilities
    jsonSmart, // for security vulnerabilities
    protobufJava, // for security vulnerabilities
    slf4j            % Runtime,
    log4jOverSlf4j   % Runtime,
    jaxb             % Runtime,
    specs2           % Test,
    catsEffectSpecs2 % Test
  )

  val pubsubDependencies = Seq(
    pubsub,
    jaxb             % Runtime,
    slf4j            % Runtime,
    log4jOverSlf4j   % Runtime,
    specs2           % Test,
    catsEffectSpecs2 % Test
  )

  val kinesisDependencies = Seq(
    kinesis,
    jaxb             % Runtime,
    slf4j            % Runtime,
    log4jOverSlf4j   % Runtime,
    stsSdk2          % Runtime,
    specs2           % Test,
    catsEffectSpecs2 % Test
  )

}
