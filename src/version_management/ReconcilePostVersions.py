"""Reconcile and post child versions to DEFAULT.

Processes all child versions in SDE geodatabases, reconciling them
to the DEFAULT version and optionally posting edits.
"""

import os
import sys

import arcpy
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sde_utils import (
    setup_logging, log_and_print, validate_paths,
    get_sde_connections, execute_sql
)

load_dotenv()


def get_child_versions(database_path):
    """Get list of non-DEFAULT versions with details.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of dicts with version name, owner, and parent
    """
    versions = []
    for version in arcpy.da.ListVersions(database_path):
        if version.name.upper() != "DBO.DEFAULT":
            versions.append({
                'name': version.name,
                'owner': version.name.split('.')[0] if '.' in version.name else 'sde',
                'parent': version.parentVersionName,
                'created': version.created if hasattr(version, 'created') else None
            })
    return versions


def reconcile_and_post(database_path, version_name, target_version="DBO.DEFAULT",
                       post=True, delete_after=False):
    """Reconcile a version to target and optionally post/delete.

    Args:
        database_path: Path to .sde connection file
        version_name: Name of version to reconcile
        target_version: Target version (default: DBO.DEFAULT)
        post: Whether to post edits after reconcile
        delete_after: Whether to delete version after successful post

    Returns:
        Dict with success status and any conflicts
    """
    result = {
        'version': version_name,
        'success': False,
        'conflicts': False,
        'posted': False,
        'deleted': False
    }

    try:
        # Set workspace before reconcile operation
        arcpy.env.workspace = database_path

        arcpy.ReconcileVersions_management(
            database_path,
            "ALL_VERSIONS",
            target_version,
            version_name,
            "LOCK_ACQUIRED",
            "ABORT_CONFLICTS" if not post else "NO_ABORT",
            "BY_OBJECT",
            "FAVOR_TARGET_VERSION",
            "POST" if post else "NO_POST",
            "KEEP_VERSION" if not delete_after else "DELETE_VERSION"
        )
        result['success'] = True
        result['posted'] = post
        result['deleted'] = delete_after

    except arcpy.ExecuteError as e:
        if "conflict" in str(e).lower():
            result['conflicts'] = True
        log_and_print(f"Error reconciling {version_name}: {e}", "error")

    return result


def process_database(database_path, sde_name, delete_after_post=False):
    """Reconcile and post all child versions for a database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        delete_after_post: Whether to delete versions after posting

    Returns:
        Dict with processing summary
    """
    log_and_print(f"Processing versions for: {sde_name}")

    versions = get_child_versions(database_path)
    if not versions:
        log_and_print(f"No child versions found in {sde_name}")
        return {'database': sde_name, 'versions_processed': 0}

    log_and_print(f"Found {len(versions)} child version(s)")

    results = []
    for version in versions:
        log_and_print(f"Reconciling: {version['name']}")
        result = reconcile_and_post(
            database_path,
            version['name'],
            post=True,
            delete_after=delete_after_post
        )
        results.append(result)

    success_count = sum(1 for r in results if r['success'])
    conflict_count = sum(1 for r in results if r['conflicts'])

    log_and_print(f"Completed: {success_count}/{len(versions)} successful, {conflict_count} conflicts")

    return {
        'database': sde_name,
        'versions_processed': len(versions),
        'successful': success_count,
        'conflicts': conflict_count,
        'results': results
    }


def main():
    """Main entry point for version reconcile/post."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    delete_after = os.environ.get('DELETE_AFTER_POST', 'false').lower() == 'true'

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "ReconcilePostVersions")

    workspace = arcpy.GetParameterAsText(0)
    if workspace:
        arcpy.env.workspace = workspace

    sde_files = get_sde_connections(connection_dir)
    if not sde_files:
        log_and_print(f"No .sde files found in {connection_dir}", "warning")
        return

    for sde_path in sde_files:
        sde_name = os.path.basename(sde_path)
        process_database(sde_path, sde_name, delete_after)

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
