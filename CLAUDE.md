# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

The Snowplow Databricks Loader is a multi-module Scala project that streams enriched Snowplow events to Databricks volumes with low latency. The codebase follows a modular architecture:

- **core**: Shared processing logic, configuration, and core functionality
- **kafka**: Azure-specific implementation using Kafka streams
- **pubsub**: GCP-specific implementation using Pub/Sub
- **kinesis**: AWS-specific implementation using Kinesis streams
- **distroless**: Distroless Docker variants of each platform module

Each platform module depends on the core module and provides a platform-specific entry point (`AzureApp`, `GcpApp`, `AwsApp`).

Key processing flow:
1. Stream consumption from cloud messaging service
2. Event transformation and Parquet serialization via `ParquetSerializer`
3. File upload to Databricks volumes via `DatabricksUploader`
4. Error handling and retry logic through `RuntimeService`

## Common Development Commands

### Build and Test
```bash
sbt compile                     # Compile all modules
sbt test                        # Run all tests
sbt "project core" test         # Run tests for specific module
sbt "project kafka" test        # Run tests for kafka module
```

### Code Formatting
```bash
sbt scalafmtAll                 # Format all Scala code
sbt scalafmtCheckAll            # Check formatting without changes
sbt scalafmtSbtCheck            # Check SBT file formatting
```

### Docker Image Creation
```bash
sbt "project kafka" docker:stage        # Stage Docker build for Kafka
sbt "project pubsub" docker:stage       # Stage Docker build for Pub/Sub
sbt "project kinesis" docker:stage      # Stage Docker build for Kinesis
sbt "project kafkaDistroless" docker:publishLocal  # Build distroless image locally
```

### Single Module Development
```bash
sbt "project core"              # Switch to core module
sbt "project kafka"             # Switch to kafka module
sbt "project pubsub"            # Switch to pubsub module
sbt "project kinesis"           # Switch to kinesis module
```

## Configuration

Configuration files are located in `config/` directory with examples for each platform:
- `config.kafka.minimal.hocon` / `config.kafka.reference.hocon` (Azure/Kafka)
- `config.pubsub.minimal.hocon` / `config.pubsub.reference.hocon` (GCP/Pub/Sub)
- `config.kinesis.minimal.hocon` / `config.kinesis.reference.hocon` (AWS/Kinesis)

The configuration system uses HOCON format and environment variable substitution for credentials.

## Key Dependencies

- **Scala**: 2.13.16
- **Cats Effect**: 3.5.4 (async/concurrent programming)
- **FS2**: 3.12.2 (functional streaming)
- **Parquet4s**: 2.19.0 (Parquet file generation)
- **Databricks SDK**: 0.62.0 (Databricks API integration)
- **Snowplow Streams**: 0.13.1 (platform-specific stream consumers)

## Testing

- Tests use Specs2 framework with Cats Effect integration
- Environment variables for tests are automatically configured in `BuildSettings.scala:78-83`
- Configuration files in `config/` are added to test classpath automatically
- Mock environments are provided in `MockEnvironment.scala`