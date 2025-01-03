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

import cats.Show
import cats.implicits.showInterpolator

import com.snowplowanalytics.snowplow.runtime.SetupExceptionMessages

sealed trait Alert
object Alert {

  final case class FailedToUploadFile(reasons: SetupExceptionMessages) extends Alert

  implicit def showAlert: Show[Alert] = Show[Alert] { case FailedToUploadFile(cause) =>
    show"Failed to upload file to Databricks volume: $cause"
  }

}
