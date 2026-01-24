"""Monitor and report on active database connections.

Provides connection monitoring, long-running connection detection,
and connection report generation for SDE geodatabases.
"""

import json
import os
import sys
import time

import arcpy
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sde_utils import (
    setup_logging, log_and_print, validate_paths,
    get_sde_connections, execute_sql
)

load_dotenv()


def get_connection_details_arcpy(database_path):
    """Get connection details using arcpy.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of connection dicts
    """
    connections = []
    try:
        users = arcpy.ListUsers(database_path)
        if not users:
            return connections

        for user in users:
            connections.append({
                'id': user.ID,
                'name': user.Name,
                'machine': user.ClientName,
                'connected_at': str(user.ConnectionTime) if hasattr(user, 'ConnectionTime') else 'Unknown',
                'is_editing': user.IsDirectConnected if hasattr(user, 'IsDirectConnected') else False
            })
    except arcpy.ExecuteError as e:
        log_and_print(f"Error listing users: {e}", "error")

    return connections


def get_connection_details_sql(database_path):
    """Get detailed connection info from SQL Server DMVs.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of connection dicts with SQL Server details
    """
    sql = """
    SELECT
        session_id,
        login_name,
        host_name,
        program_name,
        login_time,
        last_request_start_time,
        status,
        DATEDIFF(minute, login_time, GETDATE()) as connection_minutes
    FROM sys.dm_exec_sessions
    WHERE database_id = DB_ID()
      AND session_id > 50
    ORDER BY connection_minutes DESC
    """

    try:
        results = execute_sql(database_path, sql)
        if not results or results is True:
            return []

        connections = []
        for row in results:
            connections.append({
                'session_id': row[0],
                'login_name': row[1],
                'host_name': row[2],
                'program_name': row[3],
                'login_time': str(row[4]),
                'last_request': str(row[5]),
                'status': row[6],
                'connection_minutes': row[7]
            })
        return connections
    except Exception:
        return []


def check_long_running(connections, threshold_minutes):
    """Identify connections exceeding duration threshold.

    Args:
        connections: List of connection dicts
        threshold_minutes: Alert threshold in minutes

    Returns:
        List of long-running connections
    """
    long_running = []
    for conn in connections:
        minutes = conn.get('connection_minutes', 0)
        if minutes and minutes >= threshold_minutes:
            long_running.append(conn)
    return long_running


def generate_report(database_name, connections, long_running):
    """Generate formatted connection report.

    Args:
        database_name: Name of database
        connections: All connections
        long_running: Long-running connections

    Returns:
        Formatted report string
    """
    lines = [
        f"Connection Report: {database_name}",
        "=" * 60,
        f"Total Connections: {len(connections)}",
        f"Long-Running (alert): {len(long_running)}",
        ""
    ]

    if connections:
        lines.append("Active Connections:")
        lines.append("-" * 40)
        for conn in connections:
            if 'session_id' in conn:
                lines.append(
                    f"  Session {conn['session_id']}: {conn['login_name']} "
                    f"from {conn['host_name']} ({conn['connection_minutes']} min)"
                )
            else:
                lines.append(
                    f"  ID {conn['id']}: {conn['name']} from {conn['machine']}"
                )
        lines.append("")

    if long_running:
        lines.append("ALERT - Long-Running Connections:")
        lines.append("-" * 40)
        for conn in long_running:
            lines.append(
                f"  ** {conn.get('login_name', conn.get('name'))} - "
                f"{conn.get('connection_minutes', 'N/A')} minutes"
            )

    return "\n".join(lines)


def export_report(report, connections, output_dir, database_name):
    """Export report to files.

    Args:
        report: Text report string
        connections: Connection data for JSON
        output_dir: Output directory
        database_name: Database name for filename
    """
    timestr = time.strftime("%Y-%m-%d_%H%M%S")
    base_name = f"{timestr}_{database_name}_connections"

    txt_path = os.path.join(output_dir, f"{base_name}.txt")
    with open(txt_path, 'w') as f:
        f.write(report)
    log_and_print(f"Text report: {txt_path}")

    json_path = os.path.join(output_dir, f"{base_name}.json")
    with open(json_path, 'w') as f:
        json.dump({
            'database': database_name,
            'timestamp': timestr,
            'connections': connections
        }, f, indent=2, default=str)
    log_and_print(f"JSON report: {json_path}")


def process_database(database_path, sde_name, threshold_minutes, report_dir):
    """Monitor connections for a single database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        threshold_minutes: Long-connection threshold
        report_dir: Directory for reports (optional)

    Returns:
        Dict with monitoring results
    """
    log_and_print(f"Monitoring connections for: {sde_name}")

    connections = get_connection_details_sql(database_path)
    if not connections:
        connections = get_connection_details_arcpy(database_path)

    log_and_print(f"Found {len(connections)} connection(s)")

    long_running = check_long_running(connections, threshold_minutes)
    if long_running:
        log_and_print(f"WARNING: {len(long_running)} long-running connection(s) detected!", "warning")

    report = generate_report(sde_name, connections, long_running)
    print(report)

    if report_dir:
        export_report(report, connections, report_dir, sde_name.replace('.sde', ''))

    return {
        'database': sde_name,
        'total_connections': len(connections),
        'long_running': len(long_running),
        'connections': connections
    }


def main():
    """Main entry point for connection monitoring."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    threshold = int(os.environ.get('LONG_CONNECTION_THRESHOLD_MINUTES', '60'))
    report_dir = os.environ.get('REPORT_OUTPUT_DIR', log_dir)

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "MonitorConnections")

    workspace = arcpy.GetParameterAsText(0)
    if workspace:
        arcpy.env.workspace = workspace

    log_and_print(f"Long-connection threshold: {threshold} minutes")

    sde_files = get_sde_connections(connection_dir)
    if not sde_files:
        log_and_print(f"No .sde files found in {connection_dir}", "warning")
        return

    all_results = []
    for sde_path in sde_files:
        sde_name = os.path.basename(sde_path)
        result = process_database(sde_path, sde_name, threshold, report_dir)
        all_results.append(result)

    total_connections = sum(r['total_connections'] for r in all_results)
    total_long = sum(r['long_running'] for r in all_results)

    log_and_print(f"\nSummary: {total_connections} total connections, {total_long} long-running")
    log_and_print("DONE!")


if __name__ == "__main__":
    main()
