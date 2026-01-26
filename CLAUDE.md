# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Comprehensive maintenance suite for ArcGIS SDE geodatabases on SQL Server with ArcGIS Server/Portal integration. Includes version management, health monitoring, data integrity checks, and backup utilities.

## Tech Stack

- **Python** with **ArcPy** (Esri's ArcGIS Python library)
- **python-dotenv** for configuration
- **requests** for Portal/Server REST API calls
- Requires ArcGIS Pro or ArcGIS Server with valid license

## Build/Run Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run full maintenance workflow
python scripts/MaintenanceOrchestrator.py

# Run individual scripts
python src/database_maintenance/CompressRebuildAnalyze.py
python src/health_monitoring/DatabaseHealthSummary.py
python src/version_management/DeleteStaleVersions.py
```

## Test Commands

No automated tests. Scripts require ArcGIS license and SDE connections.

## Configuration

Environment variables in `.env`:
- `SDE_CONNECTION_DIR` - Directory containing `.sde` connection files
- `SDE_LOG_DIR` - Directory for log file output
- See `.env.example` for all options

## Architecture

### Directory Structure
```
src/
├── sde_utils.py              # Shared utilities (logging, SQL, auth)
├── backup/                   # Schema export
├── connection_management/    # Connection operations
├── database_maintenance/     # Compress, rebuild, analyze operations
├── data_integrity/           # Geometry/topology validation
├── health_monitoring/        # Health checks
├── server_portal/            # Server/Portal REST API
│   ├── ServerPortalMaintenance.py
│   ├── PortalBackup.py
│   └── PortalSharingAudit.py   # Sharing compliance audit
└── version_management/       # Version operations
scripts/
└── MaintenanceOrchestrator.py  # Full workflow orchestration
```

### Key Patterns

**Script Structure:**
- Each script is standalone with `main()` entry point
- Uses `setup_logging()` from sde_utils for timestamped logs
- Uses `validate_paths()` for startup validation
- Supports `arcpy.GetParameterAsText(0)` for ArcGIS tool integration

**Error Handling:**
```python
try:
    operation_func(database_path)
except arcpy.ExecuteError as e:
    logging.error(f"ArcPy error: {e}")
except Exception as e:
    logging.error(f"Unexpected error: {e}")
```

**SQL Queries:**
- Use `execute_sql()` from sde_utils for direct SQL Server queries
- Required for state lineage, delta tables, DMV queries

**REST API:**
- `get_portal_token()` / `get_ags_token()` for authentication
- Portal uses `/sharing/rest/` endpoints
- Server uses `/admin/` endpoints

### Core Functions (sde_utils.py)

- `setup_logging(log_dir, script_name)` - Configure timestamped logging
- `log_and_print(message, level)` - Dual output to log and console
- `validate_paths(**paths)` - Validate env vars and directories
- `get_sde_connections(connection_dir)` - List .sde files
- `execute_sql(database_path, sql)` - Run SQL via ArcSDESQLExecute
- `get_portal_token()` / `get_ags_token()` - REST API authentication
