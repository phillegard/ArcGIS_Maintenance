"""Comprehensive database health summary.

Combines multiple health metrics including database size, index fragmentation,
SDE repository status, and versioning health into a single report.
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
    get_sde_connections, execute_sql, format_bytes
)

load_dotenv()


def get_database_size(database_path):
    """Get database file sizes and space usage.

    Args:
        database_path: Path to .sde connection file

    Returns:
        Dict with size information
    """
    sql = """
    SELECT
        DB_NAME() as database_name,
        SUM(size * 8 * 1024) as total_bytes,
        SUM(FILEPROPERTY(name, 'SpaceUsed') * 8 * 1024) as used_bytes
    FROM sys.database_files
    """
    try:
        result = execute_sql(database_path, sql)
        if result and result is not True and len(result) > 0:
            row = result[0]
            total = row[1] or 0
            used = row[2] or 0
            return {
                'total_bytes': total,
                'used_bytes': used,
                'free_bytes': total - used,
                'used_percent': (used / total * 100) if total > 0 else 0
            }
    except Exception as e:
        log_and_print(f"Error getting database size: {e}", "error")

    return {'total_bytes': 0, 'used_bytes': 0, 'free_bytes': 0, 'used_percent': 0}


def get_index_fragmentation(database_path, threshold_percent=10):
    """Get index fragmentation above threshold.

    Args:
        database_path: Path to .sde connection file
        threshold_percent: Minimum fragmentation to report

    Returns:
        List of fragmented indexes
    """
    sql = f"""
    SELECT TOP 20
        OBJECT_NAME(ips.object_id) as table_name,
        i.name as index_name,
        ips.avg_fragmentation_in_percent as fragmentation_pct,
        ips.page_count
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > {threshold_percent}
      AND ips.page_count > 100
      AND i.name IS NOT NULL
    ORDER BY fragmentation_pct DESC
    """
    try:
        result = execute_sql(database_path, sql)
        if not result or result is True:
            return []

        indexes = []
        for row in result:
            indexes.append({
                'table': row[0],
                'index': row[1],
                'fragmentation': round(row[2], 1),
                'pages': row[3]
            })
        return indexes
    except Exception as e:
        log_and_print(f"Error getting fragmentation: {e}", "error")
        return []


def get_sde_repository_health(database_path):
    """Check SDE system tables for basic health.

    Args:
        database_path: Path to .sde connection file

    Returns:
        Dict with repository statistics
    """
    stats = {
        'registrations': 0,
        'layers': 0,
        'versions': 0,
        'states': 0
    }

    queries = [
        ("SELECT COUNT(*) FROM sde.SDE_table_registry", 'registrations'),
        ("SELECT COUNT(*) FROM sde.SDE_layers", 'layers'),
        ("SELECT COUNT(*) FROM sde.SDE_versions", 'versions'),
        ("SELECT COUNT(*) FROM sde.SDE_states", 'states')
    ]

    for sql, key in queries:
        try:
            result = execute_sql(database_path, sql)
            if result and result is not True:
                stats[key] = result[0][0] if isinstance(result[0], (list, tuple)) else result[0]
        except Exception:
            pass

    return stats


def get_table_row_counts(database_path, top_n=10):
    """Get row counts for largest tables.

    Args:
        database_path: Path to .sde connection file
        top_n: Number of tables to return

    Returns:
        List of table row counts
    """
    sql = f"""
    SELECT TOP {top_n}
        SCHEMA_NAME(t.schema_id) + '.' + t.name as table_name,
        p.rows as row_count
    FROM sys.tables t
    INNER JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0, 1)
      AND t.name NOT LIKE 'sde_%'
      AND t.name NOT LIKE 'a%'
      AND t.name NOT LIKE 'd%'
    ORDER BY p.rows DESC
    """
    try:
        result = execute_sql(database_path, sql)
        if not result or result is True:
            return []

        tables = []
        for row in result:
            tables.append({
                'table': row[0],
                'rows': row[1]
            })
        return tables
    except Exception:
        return []


def calculate_health_score(db_size, fragmentation, sde_health, frag_threshold):
    """Calculate overall health score 0-100.

    Args:
        db_size: Database size info
        fragmentation: List of fragmented indexes
        sde_health: SDE repository stats
        frag_threshold: Fragmentation warning threshold

    Returns:
        Health score and status
    """
    score = 100
    issues = []

    if db_size['used_percent'] > 90:
        score -= 30
        issues.append("Database nearly full")
    elif db_size['used_percent'] > 80:
        score -= 15
        issues.append("Database space usage high")

    high_frag_count = sum(1 for i in fragmentation if i['fragmentation'] > frag_threshold)
    if high_frag_count > 10:
        score -= 25
        issues.append(f"{high_frag_count} highly fragmented indexes")
    elif high_frag_count > 5:
        score -= 15
        issues.append(f"{high_frag_count} fragmented indexes")

    if sde_health['states'] > 50000:
        score -= 30
        issues.append("State table critically large")
    elif sde_health['states'] > 10000:
        score -= 15
        issues.append("State table growing large")

    score = max(0, score)

    if score >= 80:
        status = "GOOD"
    elif score >= 60:
        status = "FAIR"
    elif score >= 40:
        status = "POOR"
    else:
        status = "CRITICAL"

    return score, status, issues


def format_report(database_name, db_size, fragmentation, sde_health, top_tables, score, status, issues):
    """Format comprehensive health report.

    Args:
        database_name: Name of database
        db_size: Size information
        fragmentation: Fragmented indexes
        sde_health: Repository stats
        top_tables: Largest tables
        score: Health score
        status: Health status
        issues: List of issues

    Returns:
        Formatted report string
    """
    lines = [
        f"Database Health Summary: {database_name}",
        "=" * 70,
        f"Health Score: {score}/100 ({status})",
        ""
    ]

    if issues:
        lines.append("Issues:")
        for issue in issues:
            lines.append(f"  - {issue}")
        lines.append("")

    lines.extend([
        "Database Size:",
        f"  Total:     {format_bytes(db_size['total_bytes'])}",
        f"  Used:      {format_bytes(db_size['used_bytes'])} ({db_size['used_percent']:.1f}%)",
        f"  Free:      {format_bytes(db_size['free_bytes'])}",
        ""
    ])

    lines.extend([
        "SDE Repository:",
        f"  Registered Tables:  {sde_health['registrations']:,}",
        f"  Layers:             {sde_health['layers']:,}",
        f"  Versions:           {sde_health['versions']:,}",
        f"  States:             {sde_health['states']:,}",
        ""
    ])

    if fragmentation:
        lines.append(f"Fragmented Indexes (top {len(fragmentation)}):")
        lines.append(f"  {'Table':<30} {'Index':<25} {'Frag %':<8}")
        lines.append("  " + "-" * 63)
        for idx in fragmentation[:10]:
            lines.append(f"  {idx['table'][:30]:<30} {idx['index'][:25]:<25} {idx['fragmentation']:<8}")
        lines.append("")

    if top_tables:
        lines.append("Largest Tables:")
        for t in top_tables[:5]:
            lines.append(f"  {t['table']:<40} {t['rows']:>12,} rows")

    return "\n".join(lines)


def export_report(report_data, output_dir, database_name, format_type):
    """Export report to file.

    Args:
        report_data: Dict with all report data
        output_dir: Output directory
        database_name: Database name
        format_type: "json", "txt", or "html"
    """
    timestr = time.strftime("%Y-%m-%d_%H%M%S")
    base_name = f"{timestr}_{database_name}_health"

    if format_type == "json":
        path = os.path.join(output_dir, f"{base_name}.json")
        with open(path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
    else:
        path = os.path.join(output_dir, f"{base_name}.txt")
        with open(path, 'w') as f:
            f.write(report_data.get('formatted_report', ''))

    log_and_print(f"Report exported: {path}")


def process_database(database_path, sde_name, frag_threshold, output_dir, report_format):
    """Generate health summary for a database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        frag_threshold: Fragmentation warning threshold
        output_dir: Output directory for reports
        report_format: Report format (json/txt)

    Returns:
        Dict with health summary
    """
    log_and_print(f"Generating health summary for: {sde_name}")

    db_size = get_database_size(database_path)
    fragmentation = get_index_fragmentation(database_path)
    sde_health = get_sde_repository_health(database_path)
    top_tables = get_table_row_counts(database_path)

    score, status, issues = calculate_health_score(db_size, fragmentation, sde_health, frag_threshold)

    report = format_report(sde_name, db_size, fragmentation, sde_health, top_tables, score, status, issues)
    print(report)

    report_data = {
        'database': sde_name,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
        'score': score,
        'status': status,
        'issues': issues,
        'size': db_size,
        'sde_repository': sde_health,
        'fragmentation': fragmentation,
        'top_tables': top_tables,
        'formatted_report': report
    }

    if output_dir:
        export_report(report_data, output_dir, sde_name.replace('.sde', ''), report_format)

    return {
        'database': sde_name,
        'score': score,
        'status': status,
        'issues': issues
    }


def main():
    """Main entry point for database health summary."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    frag_threshold = int(os.environ.get('FRAGMENTATION_WARNING_THRESHOLD', '30'))
    report_format = os.environ.get('HEALTH_REPORT_FORMAT', 'json').lower()

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "DatabaseHealthSummary")

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
        result = process_database(sde_path, sde_name, frag_threshold, log_dir, report_format)
        results.append(result)

    log_and_print("\n" + "=" * 50)
    log_and_print("Overall Summary:")
    for r in results:
        log_and_print(f"  {r['database']}: {r['score']}/100 ({r['status']})")

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
