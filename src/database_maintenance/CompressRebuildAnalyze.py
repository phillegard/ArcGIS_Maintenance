"""ArcGIS SDE Database Maintenance Script

Performs automated maintenance operations on SDE geodatabases:
analyze, compress, rebuild indexes, analyze (second pass).
"""

import os
import sys

import arcpy
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sde_utils import (
    setup_logging, log_and_print, validate_paths,
    get_sde_connections, get_data_list
)

load_dotenv()


def analyze(database_path):
    """Run AnalyzeDatasets on all datasets in the database."""
    data_list = get_data_list(database_path)
    arcpy.AnalyzeDatasets_management(
        database_path, "NO_SYSTEM", data_list,
        "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE"
    )


def compress(database_path):
    """Compress the database to remove versioning artifacts."""
    arcpy.Compress_management(database_path)


def rebuild(database_path):
    """Rebuild indexes on all datasets in the database."""
    data_list = get_data_list(database_path)
    arcpy.RebuildIndexes_management(database_path, "NO_SYSTEM", data_list, "ALL")


def process_database(database_path, sde_name):
    """Run all maintenance operations on a single database."""
    operations = [
        ("Analyzing", analyze),
        ("Compressing", compress),
        ("Rebuilding indexes", rebuild),
        ("Analyzing", analyze),  # Second pass
    ]

    for operation_name, operation_func in operations:
        log_and_print(f"{operation_name}: {sde_name}")
        try:
            operation_func(database_path)
        except arcpy.ExecuteError as e:
            log_and_print(f"ArcPy error {operation_name.lower()} {sde_name}: {e}", "error")
            return False
        except Exception as e:
            log_and_print(f"Unexpected error {operation_name.lower()} {sde_name}: {e}", "error")
            return False

    return True


def main():
    """Main entry point for database maintenance."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "CompressRebuildAnalyze")

    workspace = arcpy.GetParameterAsText(0)
    if workspace:
        arcpy.env.workspace = workspace

    sde_files = get_sde_connections(connection_dir)
    if not sde_files:
        log_and_print(f"No .sde files found in {connection_dir}", "warning")
        return

    for sde_path in sde_files:
        sde_name = os.path.basename(sde_path)
        process_database(sde_path, sde_name)

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
