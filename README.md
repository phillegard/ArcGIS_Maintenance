# ArcGIS SDE Maintenance Suite

Comprehensive maintenance utilities for ArcGIS SDE (Spatial Database Engine) geodatabases on SQL Server, with ArcGIS Server and Portal integration.

## Features

- **Compress/Rebuild/Analyze** - Core SDE maintenance operations
- **Version Management** - Reconcile/post versions, delete stale versions, connection control
- **Connection Management** - Disconnect users, monitor connections
- **Health Monitoring** - State lineage check, delta table reports, database health summary
- **Data Integrity** - Geometry check/repair, topology validation
- **ArcGIS Server** - Service health checks, cache clearing
- **Portal Backup** - Export hosted feature services
- **Schema Backup** - XML workspace document export

## Tech Stack

- **Python** with **ArcPy** (Esri's ArcGIS Python library)
- **python-dotenv** for configuration
- **requests** for Portal/Server REST API
- Requires ArcGIS Pro or ArcGIS Server with valid license

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Create .env from template and configure
cp .env.example .env
# Edit .env with your paths and credentials
```

## Quick Start

Run the full maintenance workflow:
```bash
python scripts/MaintenanceOrchestrator.py
```

Or run individual scripts:
```bash
# Core maintenance
python CompressRebuildAnalyze.py

# Health check
python src/health_monitoring/DatabaseHealthSummary.py

# Version cleanup
python src/version_management/DeleteStaleVersions.py
```

## Project Structure

```
ArcGIS_Maintenance/
├── src/
│   ├── sde_utils.py              # Shared utilities
│   ├── version_management/       # Version operations
│   │   ├── ReconcilePostVersions.py
│   │   ├── DeleteStaleVersions.py
│   │   └── ManageConnections.py
│   ├── connection_management/    # Connection operations
│   │   ├── DisconnectUsers.py
│   │   └── MonitorConnections.py
│   ├── health_monitoring/        # Health checks
│   │   ├── StateLineageCheck.py
│   │   ├── DeltaTableReport.py
│   │   └── DatabaseHealthSummary.py
│   ├── data_integrity/           # Data validation
│   │   ├── RepairGeometry.py
│   │   └── ValidateTopology.py
│   ├── server_portal/            # Server/Portal ops
│   │   ├── ServerPortalMaintenance.py
│   │   └── PortalBackup.py
│   └── backup/
│       └── XMLWorkspaceExport.py
├── scripts/
│   └── MaintenanceOrchestrator.py
├── CompressRebuildAnalyze.py     # Original maintenance script
├── .env.example
└── requirements.txt
```

## Configuration

See `.env.example` for all configuration options. Key settings:

| Variable | Description |
|----------|-------------|
| `SDE_CONNECTION_DIR` | Directory with .sde connection files |
| `SDE_LOG_DIR` | Log output directory |
| `VERSION_MAX_AGE_DAYS` | Days before version is stale (default: 30) |
| `AGS_SERVER_URL` | ArcGIS Server admin URL (optional) |
| `PORTAL_URL` | Portal URL for backups (optional) |

## Scripts Reference

### Core Maintenance
- **CompressRebuildAnalyze.py** - Analyze → Compress → Rebuild → Analyze

### Version Management
- **ReconcilePostVersions.py** - Reconcile/post child versions to DEFAULT
- **DeleteStaleVersions.py** - Remove versions older than threshold
- **ManageConnections.py** - Block/allow connections for maintenance

### Connection Management
- **DisconnectUsers.py** - Force disconnect all users
- **MonitorConnections.py** - Report active connections, detect long-running sessions

### Health Monitoring
- **StateLineageCheck.py** - Monitor SDE states/lineage table growth
- **DeltaTableReport.py** - Report versioning delta table sizes
- **DatabaseHealthSummary.py** - Comprehensive health report with scoring

### Data Integrity
- **RepairGeometry.py** - Check and repair geometry errors
- **ValidateTopology.py** - Validate topology datasets

### Server/Portal
- **ServerPortalMaintenance.py** - Service health checks, cache clearing
- **PortalBackup.py** - Export hosted feature services to FGDB

### Backup
- **XMLWorkspaceExport.py** - Export geodatabase schema to XML

### Orchestration
- **MaintenanceOrchestrator.py** - Run complete maintenance workflow in sequence

## Maintenance Workflow

The orchestrator runs operations in this order:
1. Block new connections
2. Disconnect existing users
3. Reconcile/post versions
4. Delete stale versions
5. Compress/rebuild/analyze
6. Check/repair geometry
7. Validate topology
8. Export XML schema backup
9. Generate health report
10. Allow connections
11. ArcGIS Server health check
12. Portal backup

## Logs

All scripts write timestamped logs to `SDE_LOG_DIR`:
- `YYYY-MM-DD_ScriptName.txt` - Text logs
- `YYYY-MM-DD_HHmmss_*_report.json` - JSON reports
