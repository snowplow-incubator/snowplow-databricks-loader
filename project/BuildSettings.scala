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

// SBT
import sbt._
import sbt.io.IO
import Keys._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtdynver.DynVerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

import scala.sys.process._

object BuildSettings {

  lazy val commonSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.13.16",
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false,
    addCompilerPlugin(Dependencies.betterMonadicFor),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with docker

    Compile / resourceGenerators += Def.task {
      val license = (Compile / resourceManaged).value / "META-INF" / "LICENSE"
      IO.copyFile(file("LICENSE.md"), license)
      Seq(license)
    }.taskValue
  )

  lazy val appSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](dockerAlias, name, version),
    buildInfoPackage := "com.snowplowanalytics.snowplow.databricks",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo"),
    addExampleConfToTestCp,
    addEnvVarsForTests
  ) ++ commonSettings

  lazy val kafkaSettings = appSettings ++ Seq(
    name := "databricks-loader-kafka",
    buildInfoKeys += BuildInfoKey("cloud" -> "Azure")
  )

  lazy val pubsubSettings = appSettings ++ Seq(
    name := "databricks-loader-pubsub",
    buildInfoKeys += BuildInfoKey("cloud" -> "GCP")
  )

  lazy val kinesisSettings = appSettings ++ Seq(
    name := "databricks-loader-kinesis",
    buildInfoKeys += BuildInfoKey("cloud" -> "AWS")
  )

  lazy val addExampleConfToTestCp = Test / unmanagedClasspath += {
    if (baseDirectory.value.getPath.contains("distroless")) {
      // baseDirectory is like 'root/modules/distroless/module',
      // we're at 'module' and need to get to 'root/config/'
      baseDirectory.value.getParentFile.getParentFile.getParentFile / "config"
    } else {
      // baseDirectory is like 'root/modules/module',
      // we're at 'module' and need to get to 'root/config/'
      baseDirectory.value.getParentFile.getParentFile / "config"
    }
  }

  lazy val addEnvVarsForTests = Test / envVars := Map(
    "DATABRICKS_CLIENT_ID" -> "test-client-id",
    "DATABRICKS_CLIENT_SECRET" -> "test-secret",
    "DATABRICKS_TOKEN" -> "test-token",
    "HOSTNAME" -> "test-hostname"
  )

}
