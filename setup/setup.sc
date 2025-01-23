#!/usr/bin/env amm

import $ivy.`com.databricks:databricks-sdk-java:0.17.1`, com.google.common.collect._
import $ivy.`org.slf4j:slf4j-simple:2.0.7`

import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.{DatabricksError, UserAgent}
import com.databricks.sdk.service.workspace.{Import, ImportFormat, Language}
import com.databricks.sdk.service.catalog.VolumeType
import com.databricks.sdk.service.files.UploadRequest
import com.databricks.sdk.service.pipelines.{CreatePipeline, NotebookLibrary, PipelineCluster, PipelineLibrary}
import com.databricks.sdk.service.compute.AutoScale
import java.util.Base64
import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

UserAgent.withProduct("snowplow-loader", "0.0.0")
val w = new WorkspaceClient()

/** Values to be set by configuration **/
val catalog = sys.env("DATABRICKS_CATALOG")
val schema = sys.env("DATABRICKS_SCHEMA")
val volume = sys.env("DATABRICKS_VOLUME")
val table = sys.env("DATABRICKS_TABLE")
val clientId = sys.env("DATABRICKS_CLIENT_ID")
val sqlPath = s"/Users/$clientId/snowplow-dlt.sql"
/****/

try {
  w.volumes.create(catalog, schema, volume, VolumeType.MANAGED)
} catch {
  case de: DatabricksError if de.getMessage.contains("RESOURCE_ALREADY_EXISTS") =>
    ()
}

/* Start with an empty parquet file in the volume. This defines the schema. */

val uploadRequest = new UploadRequest()
  .setFilePath(s"/Volumes/$catalog/$schema/$volume/events/initialize.parquet")
  .setContents(new FileInputStream(new File("./initialize.parquet")))
  .setOverwrite(true)

w.files.upload(uploadRequest)


/** Create the file that defines the pipeline */

val sql = s"""
CREATE STREAMING LIVE TABLE $table
PARTITIONED BY (load_tstamp_date, event_name)
TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'load_tstamp,collector_tstamp,derived_tstamp,dvce_created_tstamp,true_tstamp')
AS
SELECT
  *,
  current_timestamp() as load_tstamp,
  date(load_tstamp) as load_tstamp_date
FROM cloud_files(
  "/Volumes/$catalog/$schema/$volume/$table",
  "parquet",
  map(
    "cloudfiles.inferColumnTypes", "false",
    "cloudfiles.includeExistingFiles", "false",
    "cloudfiles.schemaEvolutionMode", "addNewColumns",
    "cloudfiles.partitionColumns", "",
    "cloudfiles.backfillInterval", "1 day",
    "cloudfiles.useNotifications", "true",
    "datetimeRebaseMode", "CORRECTED",
    "int96RebaseMode", "CORRECTED",
    "mergeSchema", "true"
  )
)
"""

val importRequest = new Import()
  .setContent(Base64.getEncoder.encodeToString(sql.getBytes(StandardCharsets.UTF_8)))
  .setFormat(ImportFormat.SOURCE)
  .setLanguage(Language.SQL)
  .setPath(sqlPath)
  .setOverwrite(true)

w.workspace.importContent(importRequest)


/** Start the pipeline */

val createPipelineRequest = new CreatePipeline()
  .setName(s"snowplow-load-into-$catalog-$schema-$table")
  .setEdition("CORE") // Allow customers to upgrade?
  .setCatalog(catalog)
  .setTarget(schema)
  .setAllowDuplicateNames(false) // If false, deployment will fail if name conflicts with that of another pipeline.
  .setContinuous(true)
  .setChannel("Current") // DLT Release Channel that specifies which version to use.
  .setDevelopment(true)
  .setClusters {
    List(
      new PipelineCluster()
        .setLabel("default")
        .setAutoscale {
          new AutoScale()
            .setMinWorkers(1)
            .setMaxWorkers(5) // Increase this for some customers
        }

    ).asJava
  }
  .setLibraries {
    List(
      new PipelineLibrary()
        .setNotebook(new NotebookLibrary().setPath(sqlPath))
    ).asJava
  }
  .setConfiguration(Map.empty.asJava)
  .setPhoton(false) // This must be true if we change to serverless compute
  .setNotifications(List.empty.asJava) // Let customers set an email to get notifications of changes?
  .setServerless(false) // not enabled on our workspace
  //.setStorage(???)  // Not relevant if using Unity catalogs?

w.pipelines.create(createPipelineRequest)
