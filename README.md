# Snowplow Databricks Loader

[![Build Status][build-image]][build]
[![Release][release-image]][releases]
[![License][license-image]][license]

This project contains applications required to load Snowplow data into Databricks with low latency.

Check out [the example config files](./config) for how to configure your loader.

### Step 1: Run the loader

The Databricks loader reads the stream of enriched events and pushes staging files to a Databricks volume

Basic usage:
`
```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  snowplow/databricks-loader-<flavour>:0.1.0 \
  --config=/var/config.hocon \
  --iglu-config=/var/iglu.hocon
```

...where `<flavour>` is either `kinesis` (for AWS), `pubsub` (for GCP) or `kafka` (for Azure).

### Step 2: Run a Databricks Lakeflow Declarative Pipeline

Create a Pipeline in your Databricks workspace and and copy the following SQL into the associated .sql file:

```sql
CREATE STREAMING LIVE TABLE events
CLUSTER BY (load_tstamp, event_name)
TBLPROPERTIES (
  'delta.dataSkippingStatsColumns' =
      'load_tstamp,collector_tstamp,derived_tstamp,dvce_created_tstamp,true_tstamp,event_name'
)
AS SELECT
  *,
  current_timestamp() as load_tstamp
FROM cloud_files(
  "/Volumes/<CATALOG_NAME>/<VOLUME_NAME>/<SCHEMA_NAME>/events",
  "parquet",
  map(
    "cloudfiles.inferColumnTypes", "false",
    "cloudfiles.includeExistingFiles", "false", -- set to true to load files already present in the volume
    "cloudfiles.schemaEvolutionMode", "addNewColumns",
    "cloudfiles.partitionColumns", "",
    "cloudfiles.useManagedFileEvents", "true",
    "datetimeRebaseMode", "CORRECTED",
    "int96RebaseMode", "CORRECTED",
    "mergeSchema", "true"
  )
)
```

Replace `/Volumes/<CATALOG_NAME>/<VOLUME_NAME>/<SCHEMA_NAME>/events` with the correct path to your volume.


## Find out more

| Technical Docs             | Setup Guide          | Roadmap & Contributing |
|----------------------------|----------------------|------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]   |
| [Technical Docs][techdocs] | [Setup Guide][setup] | [Roadmap][roadmap]     |



## Copyright and License

Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Limited Use License Agreement][license]. _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions][faq].)_

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[setup]: https://docs.snowplow.io/docs/getting-started-on-snowplow-open-source/
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/databricks-streaming-loader/
[roadmap]: https://github.com/snowplow/snowplow/projects/7

[build-image]: https://github.com/snowplow-incubator/databricks-loader/workflows/CI/badge.svg
[build]: https://github.com/snowplow-incubator/databricks-loader/actions/workflows/ci.yml

[release-image]: https://img.shields.io/badge/release-0.1.0-blue.svg?style=flat
[releases]: https://github.com/snowplow-incubator/databricks-loader/releases

[license]: https://docs.snowplow.io/limited-use-license-1.1
[license-image]: https://img.shields.io/badge/license-Snowplow--Limited--Use-blue.svg?style=flat

[faq]: https://docs.snowplow.io/docs/contributing/limited-use-license-faq/
