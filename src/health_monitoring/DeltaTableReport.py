"""Report on delta table sizes for versioned feature classes.

Analyzes the A (adds) and D (deletes) tables that store versioned edits
to identify tables with excessive versioning overhead.
"""

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


def get_versioned_tables(database_path):
    """Get list of tables registered as versioned.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of versioned table names
    """
    sql = """
    SELECT owner, table_name, registration_id
    FROM sde.SDE_table_registry
    WHERE object_flags & 8 = 8
    ORDER BY owner, table_name
    """
    try:
        result = execute_sql(database_path, sql)
        if not result or result is True:
            return []

        tables = []
        for row in result:
            tables.append({
                'owner': row[0],
                'name': row[1],
                'registration_id': row[2]
            })
        return tables
    except Exception as e:
        log_and_print(f"Error getting versioned tables: {e}", "error")
        return []


def get_delta_table_sizes(database_path):
    """Get sizes of all delta (A and D) tables.

    Args:
        database_path: Path to .sde connection file

    Returns:
        Dict mapping base table to delta sizes
    """
    sql = """
    SELECT
        t.name as table_name,
        p.rows as row_count,
        SUM(a.total_pages) * 8 * 1024 as size_bytes
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE (t.name LIKE 'a%' OR t.name LIKE 'd%')
      AND t.name NOT LIKE 'arc%'
      AND LEN(t.name) <= 10
      AND ISNUMERIC(SUBSTRING(t.name, 2, LEN(t.name)-1)) = 1
    GROUP BY t.name, p.rows
    ORDER BY size_bytes DESC
    """
    try:
        result = execute_sql(database_path, sql)
        if not result or result is True:
            return {}

        sizes = {}
        for row in result:
            table_name = row[0]
            row_count = row[1] or 0
            size_bytes = row[2] or 0

            reg_id = table_name[1:]
            table_type = 'adds' if table_name.lower().startswith('a') else 'deletes'

            if reg_id not in sizes:
                sizes[reg_id] = {'adds_rows': 0, 'adds_bytes': 0, 'deletes_rows': 0, 'deletes_bytes': 0}

            if table_type == 'adds':
                sizes[reg_id]['adds_rows'] = row_count
                sizes[reg_id]['adds_bytes'] = size_bytes
            else:
                sizes[reg_id]['deletes_rows'] = row_count
                sizes[reg_id]['deletes_bytes'] = size_bytes

        return sizes
    except Exception as e:
        log_and_print(f"Error getting delta sizes: {e}", "error")
        return {}


def map_tables_to_deltas(versioned_tables, delta_sizes):
    """Map versioned tables to their delta table sizes.

    Args:
        versioned_tables: List of versioned table info
        delta_sizes: Dict of delta table sizes by registration_id

    Returns:
        List of tables with delta info
    """
    results = []
    for table in versioned_tables:
        reg_id = str(table['registration_id'])
        delta = delta_sizes.get(reg_id, {})

        total_bytes = delta.get('adds_bytes', 0) + delta.get('deletes_bytes', 0)
        total_rows = delta.get('adds_rows', 0) + delta.get('deletes_rows', 0)

        results.append({
            'table': f"{table['owner']}.{table['name']}",
            'registration_id': table['registration_id'],
            'adds_rows': delta.get('adds_rows', 0),
            'adds_size': delta.get('adds_bytes', 0),
            'deletes_rows': delta.get('deletes_rows', 0),
            'deletes_size': delta.get('deletes_bytes', 0),
            'total_rows': total_rows,
            'total_size': total_bytes
        })

    return sorted(results, key=lambda x: x['total_size'], reverse=True)


def identify_bloated(report_data, warning_mb, critical_mb):
    """Identify tables with excessive delta sizes.

    Args:
        report_data: List of table delta info
        warning_mb: Warning threshold in MB
        critical_mb: Critical threshold in MB

    Returns:
        Tuple of (critical_tables, warning_tables)
    """
    critical_bytes = critical_mb * 1024 * 1024
    warning_bytes = warning_mb * 1024 * 1024

    critical = [t for t in report_data if t['total_size'] >= critical_bytes]
    warning = [t for t in report_data if warning_bytes <= t['total_size'] < critical_bytes]

    return critical, warning


def format_report(database_name, report_data, critical, warning):
    """Format delta table report.

    Args:
        database_name: Name of database
        report_data: All table delta info
        critical: Critical tables
        warning: Warning tables

    Returns:
        Formatted report string
    """
    lines = [
        f"Delta Table Report: {database_name}",
        "=" * 80,
        f"Versioned Tables: {len(report_data)}",
        f"Critical (action needed): {len(critical)}",
        f"Warning: {len(warning)}",
        ""
    ]

    if critical:
        lines.append("CRITICAL - Compress these immediately:")
        lines.append("-" * 60)
        for t in critical:
            lines.append(f"  {t['table']}")
            lines.append(f"    Delta size: {format_bytes(t['total_size'])} ({t['total_rows']:,} rows)")
        lines.append("")

    if warning:
        lines.append("WARNING - Monitor these tables:")
        lines.append("-" * 60)
        for t in warning:
            lines.append(f"  {t['table']}")
            lines.append(f"    Delta size: {format_bytes(t['total_size'])} ({t['total_rows']:,} rows)")
        lines.append("")

    lines.append("Top 10 by Delta Size:")
    lines.append("-" * 60)
    lines.append(f"{'Table':<40} {'Adds':<12} {'Deletes':<12} {'Total':<12}")
    lines.append("-" * 60)
    for t in report_data[:10]:
        lines.append(
            f"{t['table'][:40]:<40} "
            f"{format_bytes(t['adds_size']):<12} "
            f"{format_bytes(t['deletes_size']):<12} "
            f"{format_bytes(t['total_size']):<12}"
        )

    return "\n".join(lines)


def process_database(database_path, sde_name, warning_mb, critical_mb, output_dir):
    """Generate delta table report for a database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        warning_mb: Warning threshold in MB
        critical_mb: Critical threshold in MB
        output_dir: Directory for JSON report

    Returns:
        Dict with report summary
    """
    log_and_print(f"Analyzing delta tables for: {sde_name}")

    versioned_tables = get_versioned_tables(database_path)
    if not versioned_tables:
        log_and_print(f"No versioned tables found in {sde_name}")
        return {'database': sde_name, 'tables': 0, 'critical': 0, 'warning': 0}

    delta_sizes = get_delta_table_sizes(database_path)
    report_data = map_tables_to_deltas(versioned_tables, delta_sizes)

    critical, warning = identify_bloated(report_data, warning_mb, critical_mb)

    report = format_report(sde_name, report_data, critical, warning)
    print(report)

    if output_dir:
        timestr = time.strftime("%Y-%m-%d_%H%M%S")
        txt_path = os.path.join(output_dir, f"{timestr}_{sde_name.replace('.sde', '')}_delta_report.txt")
        with open(txt_path, 'w') as f:
            f.write(report)
        log_and_print(f"Report saved: {txt_path}")

    return {
        'database': sde_name,
        'tables': len(versioned_tables),
        'critical': len(critical),
        'warning': len(warning)
    }


def main():
    """Main entry point for delta table report."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    warning_mb = float(os.environ.get('DELTA_SIZE_WARNING_MB', '100'))
    critical_mb = float(os.environ.get('DELTA_SIZE_CRITICAL_MB', '500'))

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "DeltaTableReport")

    log_and_print(f"Thresholds: Warning={warning_mb} MB, Critical={critical_mb} MB")

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
        result = process_database(sde_path, sde_name, warning_mb, critical_mb, log_dir)
        results.append(result)

    total_critical = sum(r['critical'] for r in results)
    total_warning = sum(r['warning'] for r in results)

    log_and_print(f"\nSummary: {total_critical} critical tables, {total_warning} warnings")
    log_and_print("DONE!")


if __name__ == "__main__":
    main()
