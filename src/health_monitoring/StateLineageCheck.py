"""Monitor SDE state lineage table for versioning overhead.

Checks the SDE_states and SDE_state_lineages tables for excessive growth
which indicates the need for database compression.
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


def get_table_count(database_path, table_name, error_label):
    """Get count of records in an SDE table.

    Args:
        database_path: Path to .sde connection file
        table_name: Name of the table to count
        error_label: Label for error messages

    Returns:
        Count of records, or -1 on error
    """
    sql = f"SELECT COUNT(*) FROM sde.{table_name}"
    try:
        result = execute_sql(database_path, sql)
        if result and result is not True:
            return result[0][0] if isinstance(result[0], (list, tuple)) else result[0]
        return 0
    except Exception as e:
        log_and_print(f"Error getting {error_label}: {e}", "error")
        return -1


def get_state_count(database_path):
    """Get count of states in SDE_states table."""
    return get_table_count(database_path, "SDE_states", "state count")


def get_lineage_count(database_path):
    """Get count of lineage records."""
    return get_table_count(database_path, "SDE_state_lineages", "lineage count")


def get_state_range(database_path):
    """Get min/max state IDs for tree depth analysis.

    Args:
        database_path: Path to .sde connection file

    Returns:
        Dict with min_state, max_state, and range
    """
    sql = """
    SELECT MIN(state_id) as min_state,
           MAX(state_id) as max_state,
           MAX(state_id) - MIN(state_id) as state_range
    FROM sde.SDE_states
    """
    try:
        result = execute_sql(database_path, sql)
        if result and result is not True and len(result) > 0:
            row = result[0]
            return {
                'min_state': row[0],
                'max_state': row[1],
                'range': row[2]
            }
        return {'min_state': 0, 'max_state': 0, 'range': 0}
    except Exception:
        return {'min_state': -1, 'max_state': -1, 'range': -1}


def get_orphan_count(database_path):
    """Count orphaned states not linked to any version.

    Args:
        database_path: Path to .sde connection file

    Returns:
        Count of orphaned states
    """
    sql = """
    SELECT COUNT(*)
    FROM sde.SDE_states s
    WHERE NOT EXISTS (
        SELECT 1 FROM sde.SDE_versions v WHERE v.state_id = s.state_id
    )
    """
    try:
        result = execute_sql(database_path, sql)
        if result and result is not True:
            return result[0][0] if isinstance(result[0], (list, tuple)) else result[0]
        return 0
    except Exception:
        return -1


def analyze_health(database_path, warning_threshold, critical_threshold):
    """Comprehensive state lineage health analysis.

    Args:
        database_path: Path to .sde connection file
        warning_threshold: Warning level for state count
        critical_threshold: Critical level for state count

    Returns:
        Dict with health analysis results
    """
    state_count = get_state_count(database_path)
    lineage_count = get_lineage_count(database_path)
    state_range = get_state_range(database_path)
    orphan_count = get_orphan_count(database_path)

    if state_count >= critical_threshold:
        status = "CRITICAL"
        recommendation = "URGENT: Run compress immediately. Performance is severely degraded."
    elif state_count >= warning_threshold:
        status = "WARNING"
        recommendation = "Schedule compress soon. State table is growing large."
    else:
        status = "OK"
        recommendation = "State table is healthy. Regular maintenance recommended."

    return {
        'state_count': state_count,
        'lineage_count': lineage_count,
        'state_range': state_range,
        'orphan_count': orphan_count,
        'status': status,
        'recommendation': recommendation
    }


def format_report(database_name, analysis):
    """Format health analysis as readable report.

    Args:
        database_name: Name of database
        analysis: Analysis results dict

    Returns:
        Formatted report string
    """
    lines = [
        f"State Lineage Health Report: {database_name}",
        "=" * 60,
        f"Status: {analysis['status']}",
        "",
        "Metrics:",
        f"  State Count:       {analysis['state_count']:,}",
        f"  Lineage Count:     {analysis['lineage_count']:,}",
        f"  State ID Range:    {analysis['state_range'].get('min_state', 'N/A')} - "
        f"{analysis['state_range'].get('max_state', 'N/A')}",
        f"  Orphaned States:   {analysis['orphan_count']:,}",
        "",
        f"Recommendation: {analysis['recommendation']}",
        ""
    ]
    return "\n".join(lines)


def process_database(database_path, sde_name, warning_threshold, critical_threshold):
    """Analyze state lineage health for a database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        warning_threshold: Warning state count
        critical_threshold: Critical state count

    Returns:
        Dict with analysis results
    """
    log_and_print(f"Analyzing state lineage for: {sde_name}")

    analysis = analyze_health(database_path, warning_threshold, critical_threshold)
    report = format_report(sde_name, analysis)
    print(report)

    if analysis['status'] == "CRITICAL":
        log_and_print(f"CRITICAL: {sde_name} needs immediate compression!", "error")
    elif analysis['status'] == "WARNING":
        log_and_print(f"WARNING: {sde_name} state table is growing", "warning")

    return {
        'database': sde_name,
        **analysis
    }


def main():
    """Main entry point for state lineage check."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    warning_threshold = int(os.environ.get('STATE_COUNT_WARNING_THRESHOLD', '10000'))
    critical_threshold = int(os.environ.get('STATE_COUNT_CRITICAL_THRESHOLD', '50000'))

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "StateLineageCheck")

    log_and_print(f"Thresholds: Warning={warning_threshold:,}, Critical={critical_threshold:,}")

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
        result = process_database(sde_path, sde_name, warning_threshold, critical_threshold)
        results.append(result)

    critical_count = sum(1 for r in results if r['status'] == 'CRITICAL')
    warning_count = sum(1 for r in results if r['status'] == 'WARNING')

    log_and_print(f"\nSummary: {critical_count} critical, {warning_count} warnings")
    log_and_print("DONE!")


if __name__ == "__main__":
    main()
