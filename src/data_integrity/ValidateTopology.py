"""Validate topology datasets and report errors.

Validates all topology datasets in SDE geodatabases and generates
error reports.
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


def get_topologies(database_path):
    """Find all topology datasets in geodatabase.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of topology paths
    """
    arcpy.env.workspace = database_path
    topologies = []

    datasets = arcpy.ListDatasets("", "Feature") or []
    for dataset in datasets:
        dataset_path = os.path.join(database_path, dataset)
        arcpy.env.workspace = dataset_path

        topos = arcpy.ListDatasets("", "Topology") or []
        for topo in topos:
            topologies.append({
                'name': topo,
                'dataset': dataset,
                'path': os.path.join(dataset_path, topo)
            })

    arcpy.env.workspace = database_path
    return topologies


def get_topology_info(topology_path):
    """Get topology properties and rules.

    Args:
        topology_path: Full path to topology

    Returns:
        Dict with topology information
    """
    try:
        desc = arcpy.Describe(topology_path)
        return {
            'name': desc.name,
            'cluster_tolerance': desc.clusterTolerance,
            'feature_class_count': len(desc.featureClassNames) if hasattr(desc, 'featureClassNames') else 0,
            'feature_classes': list(desc.featureClassNames) if hasattr(desc, 'featureClassNames') else []
        }
    except Exception:
        return {'name': '', 'cluster_tolerance': 0, 'feature_class_count': 0, 'feature_classes': []}


def validate_topology(database_path, topology_info, validate_extent="FULL_EXTENT"):
    """Validate topology and return error summary.

    Args:
        database_path: Path to .sde connection file
        topology_info: Topology info dict
        validate_extent: "FULL_EXTENT" or specific extent

    Returns:
        Dict with validation results
    """
    result = {
        'topology': topology_info['name'],
        'dataset': topology_info['dataset'],
        'validated': False,
        'error_count': 0,
        'dirty_area_count': 0
    }

    try:
        if validate_extent == "FULL_EXTENT":
            arcpy.ValidateTopology_management(topology_info['path'], "FULL_EXTENT")
        else:
            arcpy.ValidateTopology_management(topology_info['path'])

        result['validated'] = True

        desc = arcpy.Describe(topology_info['path'])
        if hasattr(desc, 'errorCount'):
            result['error_count'] = desc.errorCount

    except arcpy.ExecuteError as e:
        log_and_print(f"Error validating {topology_info['name']}: {e}", "error")

    return result


def export_topology_errors(topology_info, output_gdb):
    """Export topology errors to feature classes.

    Args:
        topology_info: Topology info dict
        output_gdb: Output geodatabase for error features

    Returns:
        Dict with export paths
    """
    if not output_gdb or not os.path.exists(output_gdb):
        return {}

    try:
        base_name = f"{topology_info['dataset']}_{topology_info['name']}"

        arcpy.ExportTopologyErrors_management(
            topology_info['path'],
            output_gdb,
            base_name
        )

        return {
            'point_errors': os.path.join(output_gdb, f"{base_name}_point"),
            'line_errors': os.path.join(output_gdb, f"{base_name}_line"),
            'poly_errors': os.path.join(output_gdb, f"{base_name}_poly")
        }
    except arcpy.ExecuteError as e:
        log_and_print(f"Error exporting topology errors: {e}", "error")
        return {}


def process_database(database_path, sde_name, validate_extent, error_gdb):
    """Validate all topologies in a database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        validate_extent: Validation extent option
        error_gdb: Output GDB for errors (optional)

    Returns:
        Dict with processing summary
    """
    log_and_print(f"Validating topologies for: {sde_name}")

    topologies = get_topologies(database_path)
    if not topologies:
        log_and_print(f"No topologies found in {sde_name}")
        return {'database': sde_name, 'topologies': 0, 'errors': 0}

    log_and_print(f"Found {len(topologies)} topology dataset(s)")

    results = []
    total_errors = 0

    for topo in topologies:
        log_and_print(f"  Validating: {topo['dataset']}/{topo['name']}")

        info = get_topology_info(topo['path'])
        result = validate_topology(database_path, topo, validate_extent)

        if result['validated']:
            log_and_print(f"    Errors: {result['error_count']}")
            total_errors += result['error_count']

            if result['error_count'] > 0 and error_gdb:
                exports = export_topology_errors(topo, error_gdb)
                result['exports'] = exports
                if exports:
                    log_and_print(f"    Errors exported to: {error_gdb}")

        result['info'] = info
        results.append(result)

    topos_with_errors = sum(1 for r in results if r['error_count'] > 0)

    log_and_print(f"Validated {len(topologies)} topologies, {topos_with_errors} with errors")

    return {
        'database': sde_name,
        'topologies': len(topologies),
        'topos_with_errors': topos_with_errors,
        'total_errors': total_errors,
        'results': results
    }


def main():
    """Main entry point for topology validation."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    validate_extent = os.environ.get('VALIDATE_EXTENT', 'FULL_EXTENT')
    error_gdb = os.environ.get('TOPOLOGY_ERROR_OUTPUT_GDB', '')

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "ValidateTopology")

    log_and_print(f"Validation extent: {validate_extent}")
    if error_gdb:
        log_and_print(f"Error export GDB: {error_gdb}")

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
        result = process_database(sde_path, sde_name, validate_extent, error_gdb)
        all_results.append(result)

    total_topos = sum(r['topologies'] for r in all_results)
    total_errors = sum(r['total_errors'] for r in all_results)

    log_and_print(f"\nSummary: {total_topos} topologies, {total_errors} total errors")
    log_and_print("DONE!")


if __name__ == "__main__":
    main()
