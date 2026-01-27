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
    get_sde_connections, execute_sql
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
    feature_classes = list(arcpy.ListFeatureClasses() or [])

    for dataset in (arcpy.ListDatasets("", "Feature") or []):
        arcpy.env.workspace = os.path.join(database_path, dataset)
        for fc in (arcpy.ListFeatureClasses() or []):
            feature_classes.append(os.path.join(dataset, fc))

    arcpy.env.workspace = database_path
    return feature_classes


def extract_table_name(fc_name):
    """Extract base table name from feature class path.

    Handles various naming formats:
    - 'Campos_Survey.DBO.Planimetrics\\Campos_Survey.DBO.Railway' -> 'Railway'
    - 'Planimetrics\\Railway' -> 'Railway'
    - 'Campos_Survey.DBO.Railway' -> 'Railway'
    - 'Railway' -> 'Railway'

    Args:
        fc_name: Feature class name (may include dataset path and schema prefix)

    Returns:
        Base table name for SQL queries
    """
    fc_part = fc_name.split('\\')[-1]
    return fc_part.split('.')[-1]


def is_feature_class_versioned(database_path, fc_name):
    """Check if a feature class is registered as versioned.

    Args:
        database_path: Path to .sde connection file
        fc_name: Feature class name (may include dataset prefix)

    Returns:
        True if versioned, False otherwise
    """
    table_name = extract_table_name(fc_name)

    sql = f"""
    SELECT COUNT(*)
    FROM sde.SDE_table_registry
    WHERE table_name = '{table_name}'
      AND object_flags & 8 = 8
    """
    try:
        result = execute_sql(database_path, sql)
        if result and result is not True:
            count = result[0][0] if isinstance(result[0], (list, tuple)) else result
            return int(count) > 0
        return False
    except Exception as e:
        log_and_print(f"Error checking versioning for {fc_name}: {e}", "warning")
        return True  # Assume versioned if we can't determine (safer)


def run_check_geometry(fc_path, fc_name):
    """Run CheckGeometry and collect results.

    Args:
        fc_path: Full path to feature class
        fc_name: Feature class name for logging

    Returns:
        Dict with check results
    """
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

        if 0 < error_count <= 100:
            with arcpy.da.SearchCursor(out_table, ["FEATURE_ID", "PROBLEM"]) as cursor:
                result['errors'] = [{'feature_id': row[0], 'problem': row[1]} for row in cursor]

        arcpy.Delete_management(out_table)

    except arcpy.ExecuteError as e:
        log_and_print(f"Error checking {fc_name}: {e}", "error")

    return result


def check_geometry(database_path, fc_name, is_versioned=None):
    """Run CheckGeometry and return problem summary.

    Handles both versioned and non-versioned feature classes.
    For versioned FCs, uses edit session approach to avoid ERROR 001259.

    Args:
        database_path: Path to .sde connection file
        fc_name: Feature class name
        is_versioned: Optional bool indicating versioning status.
                      If None, will be determined via SQL query.

    Returns:
        Dict with check results
    """
    if is_versioned is None:
        is_versioned = is_feature_class_versioned(database_path, fc_name)

    fc_path = os.path.join(database_path, fc_name)

    if not is_versioned:
        return run_check_geometry(fc_path, fc_name)

    editor = None
    result = {'feature_class': fc_name, 'checked': False, 'error_count': 0, 'errors': []}

    try:
        editor = arcpy.da.Editor(database_path)
        editor.startEditing(with_undo=False, multiuser_mode=True)
        result = run_check_geometry(fc_path, fc_name)
        editor.stopEditing(save_changes=False)

    except arcpy.ExecuteError as e:
        error_msg = str(e).lower()
        if "lock" in error_msg or "exclusive" in error_msg:
            log_and_print(f"Cannot acquire edit lock for {fc_name}: {e}", "warning")
        else:
            log_and_print(f"Edit session error for {fc_name}: {e}", "error")
    except Exception as e:
        log_and_print(f"Unexpected error checking {fc_name}: {e}", "error")
    finally:
        if editor is not None:
            try:
                if editor.isEditing:
                    editor.stopEditing(save_changes=False)
            except Exception:
                pass

    return result


def repair_geometry_versioned(database_path, fc_name):
    """Repair geometry on versioned feature class using edit session.

    Starts an edit session on DEFAULT version, performs repair,
    and saves the edit session.

    Args:
        database_path: Path to .sde connection file
        fc_name: Feature class name

    Returns:
        Tuple of (success: bool, message: str)
    """
    fc_path = os.path.join(database_path, fc_name)
    editor = None

    try:
        editor = arcpy.da.Editor(database_path)
        editor.startEditing(with_undo=False, multiuser_mode=True)
        editor.startOperation()

        try:
            arcpy.RepairGeometry_management(fc_path, "DELETE_NULL")
            editor.stopOperation()
            editor.stopEditing(save_changes=True)
            return True, "Repaired via edit session"

        except arcpy.ExecuteError as e:
            try:
                editor.abortOperation()
            except Exception:
                pass
            editor.stopEditing(save_changes=False)
            return False, f"Repair failed: {e}"

    except arcpy.ExecuteError as e:
        error_msg = str(e)
        if "lock" in error_msg.lower() or "exclusive" in error_msg.lower():
            return False, f"Cannot acquire edit lock: {e}"
        elif "001259" in error_msg:
            return False, f"Edit session approach failed: {e}"
        else:
            return False, f"Edit session error: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"
    finally:
        if editor is not None:
            try:
                if editor.isEditing:
                    editor.stopEditing(save_changes=False)
            except Exception:
                pass


def repair_geometry(database_path, fc_name, is_versioned=None):
    """Repair geometry issues in feature class.

    Handles both versioned and non-versioned feature classes.
    For versioned FCs, uses edit session approach.

    Args:
        database_path: Path to .sde connection file
        fc_name: Feature class name
        is_versioned: Optional bool indicating versioning status.
                      If None, will be determined via SQL query.

    Returns:
        True if successful, False otherwise
    """
    fc_path = os.path.join(database_path, fc_name)

    if is_versioned is None:
        is_versioned = is_feature_class_versioned(database_path, fc_name)

    if is_versioned:
        log_and_print(f"  {fc_name} is versioned - using edit session approach")
        success, message = repair_geometry_versioned(database_path, fc_name)
        if not success:
            log_and_print(f"  Skipping {fc_name}: {message}", "warning")
        return success
    else:
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
    # Check versioning once and pass it to avoid multiple SQL queries
    is_versioned = is_feature_class_versioned(database_path, fc_name)

    result = check_geometry(database_path, fc_name, is_versioned=is_versioned)
    result['is_versioned'] = is_versioned

    if result['error_count'] > 0:
        version_note = " (versioned)" if is_versioned else ""
        log_and_print(f"  {fc_name}{version_note}: {result['error_count']} geometry error(s)", "warning")

        if auto_repair:
            log_and_print(f"  Repairing {fc_name}...")
            if repair_geometry(database_path, fc_name, is_versioned=is_versioned):
                after = check_geometry(database_path, fc_name, is_versioned=is_versioned)
                result['repaired'] = True
                result['errors_after_repair'] = after['error_count']
                if after['error_count'] == 0:
                    log_and_print(f"  {fc_name}: All errors repaired")
                else:
                    log_and_print(f"  {fc_name}: {after['error_count']} errors remain", "warning")
            else:
                result['repaired'] = False
                result['repair_skipped'] = True
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
    versioned_count = 0
    skipped_count = 0

    for fc in feature_classes:
        result = process_feature_class(database_path, fc, auto_repair)
        results.append(result)
        total_errors += result['error_count']
        if result.get('is_versioned'):
            versioned_count += 1
        if result.get('repair_skipped'):
            skipped_count += 1

    fc_with_errors = sum(1 for r in results if r['error_count'] > 0)

    log_and_print(f"Checked {len(feature_classes)} feature classes ({versioned_count} versioned)")
    log_and_print(f"Found {total_errors} total geometry errors in {fc_with_errors} feature class(es)")
    if skipped_count > 0:
        log_and_print(f"Skipped {skipped_count} feature class(es) due to repair failures", "warning")

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
