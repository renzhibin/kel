# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kel (Kingbase Extract/Load) is a multi-module Spring Boot application for extracting data from Kingbase databases and loading it into target systems. The project integrates with XXL-Job for distributed task scheduling.

## Build and Run Commands

### Build the project
```bash
cd kel
mvn clean package
```

### Run the application (standalone mode)
```bash
cd kel/kel-start
java -jar target/kel-start-0.0.1-SNAPSHOT.jar --jobCode=<jobCode>
```

### Run as XXL-Job executor (daemon mode)
```bash
cd kel/kel-start
java -jar target/kel-start-0.0.1-SNAPSHOT.jar
# Application runs on port 8081, XXL-Job executor on port 9999
```

### Start XXL-Job scheduling center (development)
```bash
cd kel/deploy/xxl-job
docker compose up -d
# Access at http://localhost:8080/xxl-job-admin (admin/123456)
```

### Stop XXL-Job
```bash
cd kel/deploy/xxl-job
docker compose down
```

### Run tests
```bash
cd kel
mvn test
```

## Module Architecture

The project follows a layered multi-module architecture:

```
kel/
├── kel-dao/          # Data access layer (task execution repository)
├── kel-manager/      # Core managers (filesystem, compression, encryption, plugins)
├── kel-server/       # Business logic (services, plugins, workers)
├── kel-web/          # Web controllers (minimal, only GitPropertiesController)
└── kel-start/        # Application entry point and XXL-Job integration
```

### Module Dependencies
- `kel-start` → `kel-server` + `kel-web`
- `kel-server` → `kel-manager` + `kel-dao`
- `kel-manager` → standalone (core utilities)
- `kel-dao` → standalone (data access)

### Key Components

**kel-manager**: Provides core infrastructure managers
- `FileSystemManager`: File operations and directory management
- `CompressionManager`: GZIP compression with split threshold support
- `SmCryptoManager`: SM4 encryption for sensitive data
- `BatchNumberGenerator`: Generates batch identifiers with timestamp and sequence
- `ExtractPlugin` / `LoadPlugin`: Base interfaces for data processing plugins

**kel-server**: Business logic and orchestration
- `JobConfigService`: Loads and merges global.yaml + job-specific YAML configs
- `TaskExecutionService`: Orchestrates extract/load workflows
- `ExtractPluginRegistry` / `LoadPluginRegistry`: Plugin discovery and management
- Plugin implementations:
  - `KingbaseExtractPlugin`: Extracts data from Kingbase databases
  - `KingbaseLoadPlugin`: Loads data into Kingbase databases
  - `FileExtractPlugin`: Collects files from filesystem
  - `FileLoadPlugin`: Loads files into target systems

**kel-dao**: Task execution tracking
- `TaskExecutionRepository`: Persists task execution state
- `TaskExecutionEntity`: Task metadata (status, start/end time, error messages)
- `TaskExecutionStatus`: Enum (PENDING, RUNNING, SUCCESS, FAILED)

**kel-start**: Application bootstrap
- `KelApplication`: Main class with CommandLineRunner for standalone execution
- `XxlJobConfig`: XXL-Job executor configuration
- `KelXxlJobHandler`: Unified job handler for XXL-Job tasks

## Configuration Structure

Configuration is split into global and job-specific files:

**Global config**: `kel-start/src/main/resources/conf/dev/global.yaml`
- Concurrency settings (`default_table_concurrency`)
- Retry policy (`max_retries`, `retry_interval_sec`)
- Compression settings (`algorithm`, `split_threshold_gb`)
- Security settings (`sm4_key`, `enable_encryption`)
- Extract defaults (`work_dir`, `batch_number_format`, `encoding`)

**Job configs**: `kel-start/src/main/resources/conf/dev/jobs/<jobCode>_<extract|load>.yaml`
- Job metadata (`type`, `name`, `description`)
- Database connection (`extract_database` or `load_database`)
- Task definitions (`extract_tasks` or `load_tasks`)
- Runtime overrides (`table_concurrency`)

Job types (see `JobType` enum):
- `EXTRACT_KINGBASE`: Extract from Kingbase database
- `FILE_EXTRACT`: Collect files from filesystem
- `KINGBASE_LOAD`: Load into Kingbase database
- `FILE_LOAD`: Load files into target system

## XXL-Job Integration

The application acts as an XXL-Job executor with a unified job handler:

**JobHandler name**: `kelJobHandler`

**Execution parameters**:
- `extract:<jobCode>` - Execute extract workflow for the job
- `load:<jobCode>` - Execute load workflow for the job
- `<jobCode>` - Defaults to extract workflow

Example: To extract data using the `demo` job, configure XXL-Job task with:
- Executor: `kel-executor`
- JobHandler: `kelJobHandler`
- Execution parameter: `extract:demo` or just `demo`

The handler parses the parameter, loads the corresponding YAML config, and invokes `TaskExecutionService.executeExtract()` or `executeLoad()`.

## Development Notes

**Database driver**: Currently uses PostgreSQL driver as a placeholder for Kingbase. Replace with official Kingbase driver when available (see `kel-server/pom.xml:34-39`).

**Configuration profiles**: The project uses `dev` profile by default (`kel.conf.base-dir: classpath:conf/dev`). Add additional profiles (test, prod) by creating corresponding directories under `conf/`.

**Adding new job types**:
1. Create YAML config in `conf/dev/jobs/<jobCode>_<extract|load>.yaml`
2. Implement plugin if needed (extend `ExtractPlugin` or `LoadPlugin`)
3. Register plugin in `ExtractPluginRegistry` or `LoadPluginRegistry`
4. Configure XXL-Job task with appropriate execution parameter

**Plugin architecture**: Plugins are discovered via Spring component scanning. Each plugin declares which `JobType` it handles. The registry maps job types to plugin implementations at runtime.
