"""Delete versions older than a specified age threshold.

Cleans up stale versions in SDE geodatabases based on age
and configurable exclusion patterns.
"""

import os
import re
import sys
from datetime import datetime, timedelta

import arcpy
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sde_utils import (
    setup_logging, log_and_print, validate_paths,
    get_sde_connections, execute_sql
)

load_dotenv()


def get_version_details(database_path):
    """Get versions with age information via SQL.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of dicts with version name, owner, age_days
    """
    sql = """
    SELECT name, owner, creation_time,
           DATEDIFF(day, creation_time, GETDATE()) as age_days
    FROM sde.SDE_versions
    WHERE name != 'DEFAULT'
    ORDER BY age_days DESC
    """

    try:
        results = execute_sql(database_path, sql)
        if not results or results is True:
            return []

        versions = []
        for row in results:
            versions.append({
                'name': f"{row[1]}.{row[0]}" if row[1] else row[0],
                'owner': row[1],
                'created': row[2],
                'age_days': row[3]
            })
        return versions
    except Exception:
        return get_version_details_arcpy(database_path)


def get_version_details_arcpy(database_path):
    """Fallback: get version details using arcpy.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of dicts with version name, owner, age_days
    """
    versions = []
    now = datetime.now()

    for version in arcpy.da.ListVersions(database_path):
        if version.name.upper() == "SDE.DEFAULT":
            continue

        age_days = 0
        if hasattr(version, 'created') and version.created:
            age_days = (now - version.created).days

        versions.append({
            'name': version.name,
            'owner': version.name.split('.')[0] if '.' in version.name else 'sde',
            'created': version.created if hasattr(version, 'created') else None,
            'age_days': age_days
        })

    return sorted(versions, key=lambda x: x['age_days'], reverse=True)


def filter_stale_versions(versions, max_age_days, exclude_patterns=None):
    """Filter versions older than threshold, excluding patterns.

    Args:
        versions: List of version dicts
        max_age_days: Maximum age in days
        exclude_patterns: List of patterns to exclude (e.g., ["QA_", "PROD_"])

    Returns:
        List of versions to delete
    """
    stale = []
    exclude_patterns = exclude_patterns or []

    for version in versions:
        if version['age_days'] < max_age_days:
            continue

        excluded = False
        for pattern in exclude_patterns:
            if pattern.lower() in version['name'].lower():
                excluded = True
                break

        if not excluded:
            stale.append(version)

    return stale


def delete_version(database_path, version_name):
    """Delete a single version.

    Args:
        database_path: Path to .sde connection file
        version_name: Full version name to delete

    Returns:
        True if successful, False otherwise
    """
    try:
        arcpy.DeleteVersion_management(database_path, version_name)
        return True
    except arcpy.ExecuteError as e:
        log_and_print(f"Error deleting {version_name}: {e}", "error")
        return False


def process_database(database_path, sde_name, max_age_days, exclude_patterns):
    """Delete stale versions for a database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        max_age_days: Maximum version age in days
        exclude_patterns: Patterns to exclude from deletion

    Returns:
        Dict with processing summary
    """
    log_and_print(f"Checking stale versions in: {sde_name}")

    versions = get_version_details(database_path)
    if not versions:
        log_and_print(f"No child versions found in {sde_name}")
        return {'database': sde_name, 'deleted': 0, 'skipped': 0}

    stale = filter_stale_versions(versions, max_age_days, exclude_patterns)
    if not stale:
        log_and_print(f"No stale versions (>{max_age_days} days) in {sde_name}")
        return {'database': sde_name, 'deleted': 0, 'skipped': 0}

    log_and_print(f"Found {len(stale)} stale version(s) to delete")

    deleted = 0
    for version in stale:
        log_and_print(f"Deleting: {version['name']} (age: {version['age_days']} days)")
        if delete_version(database_path, version['name']):
            deleted += 1

    log_and_print(f"Deleted {deleted}/{len(stale)} versions")

    return {
        'database': sde_name,
        'deleted': deleted,
        'skipped': len(stale) - deleted
    }


def main():
    """Main entry point for stale version cleanup."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    max_age_days = int(os.environ.get('VERSION_MAX_AGE_DAYS', '30'))
    exclude_str = os.environ.get('VERSION_EXCLUDE_PATTERNS', '')
    exclude_patterns = [p.strip() for p in exclude_str.split(',') if p.strip()]

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "DeleteStaleVersions")

    log_and_print(f"Max age threshold: {max_age_days} days")
    if exclude_patterns:
        log_and_print(f"Excluding patterns: {exclude_patterns}")

    workspace = arcpy.GetParameterAsText(0)
    if workspace:
        arcpy.env.workspace = workspace

    sde_files = get_sde_connections(connection_dir)
    if not sde_files:
        log_and_print(f"No .sde files found in {connection_dir}", "warning")
        return

    for sde_path in sde_files:
        sde_name = os.path.basename(sde_path)
        process_database(sde_path, sde_name, max_age_days, exclude_patterns)

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
