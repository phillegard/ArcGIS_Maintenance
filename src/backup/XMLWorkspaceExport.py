"""Export geodatabase schema as XML workspace document for backup.

Creates XML workspace documents that can be used to recreate
geodatabase schema (and optionally data).
"""

import glob
import os
import sys
import time

import arcpy
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sde_utils import (
    setup_logging, log_and_print, validate_paths,
    get_sde_connections
)

load_dotenv()


def export_schema(database_path, output_path, include_data=False):
    """Export geodatabase schema to XML workspace document.

    Args:
        database_path: Path to .sde connection file
        output_path: Output XML file path
        include_data: Whether to include data (default: schema only)

    Returns:
        True if successful, False otherwise
    """
    export_option = "DATA" if include_data else "SCHEMA_ONLY"

    try:
        arcpy.ExportXMLWorkspaceDocument_management(
            database_path,
            output_path,
            export_option,
            "BINARY",
            "NO_METADATA"
        )
        return True
    except arcpy.ExecuteError as e:
        log_and_print(f"Export error: {e}", "error")
        return False


def validate_xml(xml_path):
    """Basic validation that XML file exists and has content.

    Args:
        xml_path: Path to XML file

    Returns:
        True if valid, False otherwise
    """
    if not os.path.exists(xml_path):
        return False

    size = os.path.getsize(xml_path)
    if size < 1000:
        log_and_print(f"Warning: XML file unusually small ({size} bytes)", "warning")
        return False

    return True


def rotate_backups(backup_dir, database_name, max_backups):
    """Rotate old backups, keeping only max_backups.

    Args:
        backup_dir: Backup directory
        database_name: Database name for pattern matching
        max_backups: Number of backups to retain
    """
    pattern = os.path.join(backup_dir, f"*_{database_name}_schema.xml")
    backups = sorted(glob.glob(pattern), reverse=True)

    if len(backups) > max_backups:
        for old_backup in backups[max_backups:]:
            try:
                os.remove(old_backup)
                log_and_print(f"Rotated old backup: {os.path.basename(old_backup)}")
            except OSError as e:
                log_and_print(f"Error removing old backup: {e}", "error")


def process_database(database_path, sde_name, output_dir, include_data, max_backups):
    """Export schema for a single database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        output_dir: Output directory for XML
        include_data: Whether to include data
        max_backups: Number of backups to retain

    Returns:
        Dict with export results
    """
    log_and_print(f"Exporting schema for: {sde_name}")

    db_name = sde_name.replace('.sde', '')
    timestr = time.strftime("%Y-%m-%d_%H%M%S")
    output_filename = f"{timestr}_{db_name}_schema.xml"
    output_path = os.path.join(output_dir, output_filename)

    success = export_schema(database_path, output_path, include_data)

    if success:
        if validate_xml(output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            log_and_print(f"Exported: {output_filename} ({size_mb:.2f} MB)")

            rotate_backups(output_dir, db_name, max_backups)
        else:
            log_and_print(f"Export validation failed", "error")
            success = False
    else:
        log_and_print(f"Export failed for {sde_name}", "error")

    return {
        'database': sde_name,
        'success': success,
        'path': output_path if success else None
    }


def main():
    """Main entry point for XML workspace export."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    backup_dir = os.environ.get('XML_BACKUP_DIR', log_dir)
    include_data = os.environ.get('XML_INCLUDE_DATA', 'false').lower() == 'true'
    max_backups = int(os.environ.get('XML_BACKUP_RETENTION', '7'))

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "XMLWorkspaceExport")

    if not os.path.isdir(backup_dir):
        os.makedirs(backup_dir, exist_ok=True)

    log_and_print(f"Backup directory: {backup_dir}")
    log_and_print(f"Include data: {include_data}")
    log_and_print(f"Retention: {max_backups} backups")

    workspace = arcpy.GetParameterAsText(0)
    if workspace:
        arcpy.env.workspace = workspace

    sde_files = get_sde_connections(connection_dir)
    if not sde_files:
        log_and_print(f"No .sde files found in {connection_dir}", "warning")
        return

    results = []
    for sde_path in sde_files:
        sde_name = os.path.basename(sde_path)
        result = process_database(sde_path, sde_name, backup_dir, include_data, max_backups)
        results.append(result)

    success_count = sum(1 for r in results if r['success'])
    log_and_print(f"\nExported {success_count}/{len(results)} databases")
    log_and_print("DONE!")


if __name__ == "__main__":
    main()
