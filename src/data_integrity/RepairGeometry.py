"""Check and repair geometry for all feature classes.

Identifies and optionally repairs invalid geometries in SDE geodatabases.
"""

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


def get_feature_classes(database_path):
    """Get all feature classes including those in feature datasets.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of feature class paths
    """
    arcpy.env.workspace = database_path
    feature_classes = []

    standalone_fcs = arcpy.ListFeatureClasses() or []
    for fc in standalone_fcs:
        feature_classes.append(fc)

    datasets = arcpy.ListDatasets("", "Feature") or []
    for dataset in datasets:
        arcpy.env.workspace = os.path.join(database_path, dataset)
        dataset_fcs = arcpy.ListFeatureClasses() or []
        for fc in dataset_fcs:
            feature_classes.append(os.path.join(dataset, fc))

    arcpy.env.workspace = database_path
    return feature_classes


def check_geometry(database_path, fc_name):
    """Run CheckGeometry and return problem summary.

    Args:
        database_path: Path to .sde connection file
        fc_name: Feature class name

    Returns:
        Dict with check results
    """
    fc_path = os.path.join(database_path, fc_name)

    result = {
        'feature_class': fc_name,
        'checked': False,
        'error_count': 0,
        'errors': []
    }

    try:
        out_table = arcpy.CreateScratchName("geom_check", data_type="ArcInfoTable",
                                            workspace=arcpy.env.scratchGDB)
        arcpy.CheckGeometry_management(fc_path, out_table)

        error_count = int(arcpy.GetCount_management(out_table)[0])
        result['checked'] = True
        result['error_count'] = error_count

        if error_count > 0 and error_count <= 100:
            with arcpy.da.SearchCursor(out_table, ["FEATURE_ID", "PROBLEM"]) as cursor:
                for row in cursor:
                    result['errors'].append({
                        'feature_id': row[0],
                        'problem': row[1]
                    })

        arcpy.Delete_management(out_table)

    except arcpy.ExecuteError as e:
        log_and_print(f"Error checking {fc_name}: {e}", "error")

    return result


def repair_geometry(database_path, fc_name):
    """Repair geometry issues in feature class.

    Args:
        database_path: Path to .sde connection file
        fc_name: Feature class name

    Returns:
        True if successful, False otherwise
    """
    fc_path = os.path.join(database_path, fc_name)

    try:
        arcpy.RepairGeometry_management(fc_path, "DELETE_NULL")
        return True
    except arcpy.ExecuteError as e:
        log_and_print(f"Error repairing {fc_name}: {e}", "error")
        return False


def process_feature_class(database_path, fc_name, auto_repair):
    """Check and optionally repair a single feature class.

    Args:
        database_path: Path to .sde connection file
        fc_name: Feature class name
        auto_repair: Whether to automatically repair issues

    Returns:
        Dict with processing results
    """
    result = check_geometry(database_path, fc_name)

    if result['error_count'] > 0:
        log_and_print(f"  {fc_name}: {result['error_count']} geometry error(s)", "warning")

        if auto_repair:
            log_and_print(f"  Repairing {fc_name}...")
            if repair_geometry(database_path, fc_name):
                after = check_geometry(database_path, fc_name)
                result['repaired'] = True
                result['errors_after_repair'] = after['error_count']
                if after['error_count'] == 0:
                    log_and_print(f"  {fc_name}: All errors repaired")
                else:
                    log_and_print(f"  {fc_name}: {after['error_count']} errors remain", "warning")
            else:
                result['repaired'] = False
    else:
        result['repaired'] = False

    return result


def process_database(database_path, sde_name, auto_repair, report_dir):
    """Check/repair all feature classes in a database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        auto_repair: Whether to automatically repair
        report_dir: Directory for report output

    Returns:
        Dict with processing summary
    """
    log_and_print(f"Checking geometry for: {sde_name}")

    feature_classes = get_feature_classes(database_path)
    if not feature_classes:
        log_and_print(f"No feature classes found in {sde_name}")
        return {'database': sde_name, 'checked': 0, 'errors': 0}

    log_and_print(f"Found {len(feature_classes)} feature class(es)")

    results = []
    total_errors = 0

    for fc in feature_classes:
        result = process_feature_class(database_path, fc, auto_repair)
        results.append(result)
        total_errors += result['error_count']

    fc_with_errors = sum(1 for r in results if r['error_count'] > 0)

    log_and_print(f"Checked {len(feature_classes)} feature classes")
    log_and_print(f"Found {total_errors} total geometry errors in {fc_with_errors} feature class(es)")

    if report_dir and total_errors > 0:
        timestr = time.strftime("%Y-%m-%d_%H%M%S")
        report_path = os.path.join(report_dir, f"{timestr}_{sde_name.replace('.sde', '')}_geometry.txt")
        with open(report_path, 'w') as f:
            f.write(f"Geometry Report: {sde_name}\n")
            f.write("=" * 60 + "\n\n")
            for r in results:
                if r['error_count'] > 0:
                    f.write(f"{r['feature_class']}: {r['error_count']} errors\n")
                    for err in r.get('errors', [])[:20]:
                        f.write(f"  FID {err['feature_id']}: {err['problem']}\n")
                    if r['error_count'] > 20:
                        f.write(f"  ... and {r['error_count'] - 20} more\n")
                    f.write("\n")
        log_and_print(f"Report saved: {report_path}")

    return {
        'database': sde_name,
        'checked': len(feature_classes),
        'fc_with_errors': fc_with_errors,
        'total_errors': total_errors,
        'results': results
    }


def main():
    """Main entry point for geometry check/repair."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    auto_repair = os.environ.get('AUTO_REPAIR', 'false').lower() == 'true'
    report_dir = os.environ.get('GEOMETRY_REPORT_DIR', log_dir)

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "RepairGeometry")

    log_and_print(f"Auto-repair: {'enabled' if auto_repair else 'disabled (report only)'}")

    workspace = arcpy.GetParameterAsText(0)
    if workspace:
        arcpy.env.workspace = workspace

    sde_files = get_sde_connections(connection_dir)
    if not sde_files:
        log_and_print(f"No .sde files found in {connection_dir}", "warning")
        return

    all_results = []
    for sde_path in sde_files:
        sde_name = os.path.basename(sde_path)
        result = process_database(sde_path, sde_name, auto_repair, report_dir)
        all_results.append(result)

    total_errors = sum(r['total_errors'] for r in all_results)
    log_and_print(f"\nTotal geometry errors across all databases: {total_errors}")
    log_and_print("DONE!")


if __name__ == "__main__":
    main()
