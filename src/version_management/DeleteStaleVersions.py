"""Delete versions older than a specified age threshold.

Cleans up stale versions in SDE geodatabases based on age
and configurable exclusion patterns.
"""

import os
import sys
from datetime import datetime

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
    SELECT name, owner, parent_name, creation_time,
           DATEDIFF(day, creation_time, GETDATE()) as age_days
    FROM dbo.SDE_versions
    WHERE name != 'DEFAULT'
    """

    try:
        results = execute_sql(database_path, sql)
        if not results or results is True:
            return []

        versions = []
        for row in results:
            owner = row[1] if row[1] else ''
            parent_raw = row[2]
            # Format parent with owner prefix to match version name format
            if parent_raw and parent_raw.upper() != 'DEFAULT':
                parent = f"{owner}.{parent_raw}" if owner else parent_raw
            else:
                parent = parent_raw

            versions.append({
                'name': f"{owner}.{row[0]}" if owner else row[0],
                'owner': owner,
                'parent': parent,
                'created': row[3],
                'age_days': row[4]
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
        if version.name.upper() == "DBO.DEFAULT":
            continue

        age_days = 0
        if hasattr(version, 'created') and version.created:
            age_days = (now - version.created).days

        versions.append({
            'name': version.name,
            'owner': version.name.split('.')[0] if '.' in version.name else 'sde',
            'parent': version.parentVersionName,
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
    exclude_patterns = exclude_patterns or []

    def is_stale_and_not_excluded(version):
        if version['age_days'] < max_age_days:
            return False
        version_name_lower = version['name'].lower()
        return not any(p.lower() in version_name_lower for p in exclude_patterns)

    return [v for v in versions if is_stale_and_not_excluded(v)]


def order_versions_for_deletion(versions):
    """Order versions so children are deleted before parents.

    Args:
        versions: List of version dicts with 'name' and 'parent' keys

    Returns:
        List of versions ordered for safe deletion (leaves first)
    """
    version_names = {v['name'] for v in versions}
    version_map = {v['name']: v for v in versions}

    # Build children lookup
    children = {v['name']: [] for v in versions}
    for v in versions:
        parent = v.get('parent')
        if parent and parent in children:
            children[parent].append(v['name'])

    # Topological sort: process leaves first
    ordered = []
    remaining = set(version_names)

    while remaining:
        # Find versions with no remaining children
        leaves = [name for name in remaining
                  if all(child not in remaining for child in children[name])]

        if not leaves:
            # Circular dependency or external parent - add remaining
            ordered.extend(version_map[name] for name in remaining)
            break

        for name in leaves:
            ordered.append(version_map[name])
            remaining.remove(name)

    return ordered


def has_non_stale_children(version_name, stale_names, all_versions):
    """Check if version has children that aren't in the stale list.

    Args:
        version_name: Name of version to check
        stale_names: Set of version names marked for deletion
        all_versions: All versions with parent info

    Returns:
        True if version has children not in stale list
    """
    for v in all_versions:
        if v.get('parent') == version_name and v['name'] not in stale_names:
            return True
    return False


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

    # Filter out parents with non-stale children
    stale_names = {v['name'] for v in stale}
    for v in stale[:]:
        if has_non_stale_children(v['name'], stale_names, versions):
            log_and_print(
                f"Skipping {v['name']} - has non-stale child versions",
                "warning"
            )
            stale.remove(v)

    if not stale:
        log_and_print(f"No deletable stale versions in {sde_name}")
        return {'database': sde_name, 'deleted': 0, 'skipped': 0}

    # Order versions for safe deletion (children before parents)
    stale = order_versions_for_deletion(stale)

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
