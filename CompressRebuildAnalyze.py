"""ArcGIS SDE Database Maintenance Script

Performs automated maintenance operations on SDE geodatabases:
analyze, compress, rebuild indexes, analyze (second pass).
"""

import os
import logging
import time

import arcpy
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_data_list(database_path):
    """Build list of all tables, feature classes, rasters, and datasets in a database."""
    arcpy.env.workspace = database_path
    data_list = arcpy.ListTables() + arcpy.ListFeatureClasses() + arcpy.ListRasters()

    for dataset in arcpy.ListDatasets("", "Feature"):
        arcpy.env.workspace = os.path.join(database_path, dataset)
        data_list += arcpy.ListFeatureClasses() + arcpy.ListDatasets()

    return data_list


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


def setup_logging(log_dir):
    """Configure logging with timestamped filename."""
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    timestr = time.strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{timestr}_CompressRebuildAnalyze.txt")

    logging.basicConfig(
        filename=log_file,
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO
    )


def validate_paths(connection_dir, log_dir):
    """Validate that required directories exist."""
    if not connection_dir:
        raise ValueError("SDE_CONNECTION_DIR environment variable is not set")
    if not log_dir:
        raise ValueError("SDE_LOG_DIR environment variable is not set")
    if not os.path.isdir(connection_dir):
        raise ValueError(f"Connection directory not found: {connection_dir}")


def process_database(database_path, sde_name):
    """Run all maintenance operations on a single database."""
    operations = [
        ("Analyzing", analyze),
        ("Compressing", compress),
        ("Rebuilding indexes", rebuild),
        ("Analyzing", analyze),  # Second pass
    ]

    for operation_name, operation_func in operations:
        logging.info(f"{operation_name}: {sde_name}")
        print(f"{operation_name}: {sde_name}")
        try:
            operation_func(database_path)
        except arcpy.ExecuteError as e:
            logging.error(f"ArcPy error {operation_name.lower()} {sde_name}: {e}")
            print(f"Error {operation_name.lower()}: {sde_name}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error {operation_name.lower()} {sde_name}: {e}")
            print(f"Error {operation_name.lower()}: {sde_name}")
            return False

    return True


def main():
    """Main entry point for database maintenance."""
    # Load configuration from environment
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')

    # Validate paths
    validate_paths(connection_dir, log_dir)

    # Setup logging
    setup_logging(log_dir)

    # Optional: workspace parameter for ArcGIS tool integration
    workspace = arcpy.GetParameterAsText(0)
    if workspace:
        arcpy.env.workspace = workspace

    # Process each SDE connection file
    sde_files = [f for f in os.listdir(connection_dir) if f.endswith('.sde')]

    if not sde_files:
        logging.warning(f"No .sde files found in {connection_dir}")
        print(f"No .sde files found in {connection_dir}")
        return

    for sde in sde_files:
        database_path = os.path.join(connection_dir, sde)
        process_database(database_path, sde)

    logging.info("DONE!")
    print("DONE!")


if __name__ == "__main__":
    main()
