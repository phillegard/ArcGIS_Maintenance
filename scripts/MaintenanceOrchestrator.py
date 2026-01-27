"""Master orchestration script for complete SDE maintenance workflow.

Runs the full maintenance sequence in the correct order:
1. Block connections
2. Disconnect users
3. Reconcile/post versions
4. Delete stale versions
5. Compress/rebuild/analyze
6. Check/repair geometry
7. Validate topology
8. Export XML schema backup
9. Generate health report
10. Allow connections
11. Clear service caches
12. Backup Portal hosted services
13. Portal sharing audit
"""

import os
import subprocess
import sys
import time

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.sde_utils import setup_logging, log_and_print, validate_paths

load_dotenv()


SCRIPT_SEQUENCE = [
    {
        'name': 'Block Connections',
        'module': 'src.version_management.ManageConnections',
        'critical': True,
        'args': ['block']
    },
    {
        'name': 'Disconnect Users',
        'module': 'src.connection_management.DisconnectUsers',
        'critical': True
    },
    {
        'name': 'Reconcile/Post Versions',
        'module': 'src.version_management.ReconcilePostVersions',
        'critical': False
    },
    {
        'name': 'Delete Stale Versions',
        'module': 'src.version_management.DeleteStaleVersions',
        'critical': False
    },
    {
        'name': 'Compress/Rebuild/Analyze',
        'module': 'src.database_maintenance.CompressRebuildAnalyze',
        'critical': True
    },
    {
        'name': 'Check Geometry',
        'module': 'src.data_integrity.RepairGeometry',
        'critical': False
    },
    {
        'name': 'Validate Topology',
        'module': 'src.data_integrity.ValidateTopology',
        'critical': False
    },
    {
        'name': 'Export XML Schema',
        'module': 'src.backup.XMLWorkspaceExport',
        'critical': False
    },
    {
        'name': 'Generate Health Report',
        'module': 'src.health_monitoring.DatabaseHealthSummary',
        'critical': False
    },
    {
        'name': 'Allow Connections',
        'module': 'src.version_management.ManageConnections',
        'critical': True,
        'args': ['allow']
    },
    {
        'name': 'Server Health Check',
        'module': 'src.server_portal.ServerPortalMaintenance',
        'critical': False,
        'requires_config': ['AGS_SERVER_URL', 'AGS_ADMIN_USER', 'AGS_ADMIN_PASSWORD']
    },
    {
        'name': 'Portal Backup',
        'module': 'src.server_portal.PortalBackup',
        'critical': False,
        'requires_config': ['PORTAL_URL', 'PORTAL_ADMIN_USER', 'PORTAL_ADMIN_PASSWORD', 'PORTAL_BACKUP_DIR']
    },
    {
        'name': 'Portal Sharing Audit',
        'module': 'src.server_portal.PortalSharingAudit',
        'critical': False,
        'requires_config': ['PORTAL_URL', 'PORTAL_ADMIN_USER', 'PORTAL_ADMIN_PASSWORD']
    }
]


def check_config_available(required_vars):
    """Check if required config variables are set.

    Args:
        required_vars: List of environment variable names

    Returns:
        True if all are set, False otherwise
    """
    return all(os.environ.get(var) for var in required_vars)


def run_script(script_path, args=None, timeout=18000):
    """Run a Python script.

    Args:
        script_path: Path to script (module path like 'src.module' or direct path)
        args: Optional command line arguments
        timeout: Timeout in seconds (default: 5 hours)

    Returns:
        Tuple of (success, output)
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    if '.' in script_path and not script_path.endswith('.py'):
        full_path = os.path.join(project_root, script_path.replace('.', os.sep) + '.py')
    else:
        full_path = os.path.join(project_root, script_path)

    cmd = [sys.executable, full_path]
    if args:
        cmd.extend(args)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=project_root
        )
        return result.returncode == 0, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        hours = timeout // 3600
        return False, f"Script timed out after {hours} hour(s)"
    except Exception as e:
        return False, str(e)


def run_maintenance_sequence(steps=None, skip_steps=None):
    """Run the maintenance sequence.

    Args:
        steps: List of step names to run (None = all)
        skip_steps: List of step names to skip

    Returns:
        Dict with results summary
    """
    skip_steps = skip_steps or []

    results = {
        'start_time': time.strftime("%Y-%m-%d %H:%M:%S"),
        'steps': [],
        'success': True,
        'critical_failure': False
    }

    for step in SCRIPT_SEQUENCE:
        step_name = step['name']

        if steps and step_name not in steps:
            continue

        if step_name in skip_steps:
            log_and_print(f"SKIPPING: {step_name}")
            results['steps'].append({
                'name': step_name,
                'status': 'skipped',
                'duration': 0
            })
            continue

        if 'requires_config' in step:
            if not check_config_available(step['requires_config']):
                log_and_print(f"SKIPPING: {step_name} (missing configuration)")
                results['steps'].append({
                    'name': step_name,
                    'status': 'skipped',
                    'reason': 'missing configuration'
                })
                continue

        log_and_print(f"\n{'='*60}")
        log_and_print(f"RUNNING: {step_name}")
        log_and_print(f"{'='*60}")

        start_time = time.time()

        if 'module' in step:
            success, output = run_script(step['module'], step.get('args'))
        elif 'script' in step:
            success, output = run_script(step['script'], timeout=3600)
        else:
            success, output = False, "No module or script specified"

        duration = time.time() - start_time

        step_result = {
            'name': step_name,
            'status': 'success' if success else 'failed',
            'duration': round(duration, 1),
            'output_preview': output[:500] if output else ''
        }
        results['steps'].append(step_result)

        if success:
            log_and_print(f"COMPLETED: {step_name} ({duration:.1f}s)")
        else:
            log_and_print(f"FAILED: {step_name}", "error")
            results['success'] = False

            if step.get('critical', False):
                log_and_print("Critical step failed - stopping maintenance", "error")
                results['critical_failure'] = True
                break

    results['end_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
    return results


def format_summary(results):
    """Format results as summary report.

    Args:
        results: Results dict

    Returns:
        Formatted string
    """
    lines = [
        "",
        "=" * 60,
        "MAINTENANCE SUMMARY",
        "=" * 60,
        f"Start: {results['start_time']}",
        f"End: {results['end_time']}",
        f"Overall Status: {'SUCCESS' if results['success'] else 'FAILED'}",
        "",
        "Steps:"
    ]

    status_icons = {'success': '+', 'failed': '-', 'skipped': 'o'}
    for step in results['steps']:
        icon = status_icons.get(step['status'], 'o')
        duration = f"({step.get('duration', 0):.1f}s)" if step.get('duration') else ""
        lines.append(f"  [{icon}] {step['name']} {duration}")

    return "\n".join(lines)


def main():
    """Main entry point for maintenance orchestrator."""
    log_dir = os.environ.get('SDE_LOG_DIR')
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')

    if not connection_dir:
        print("Error: SDE_CONNECTION_DIR environment variable is required")
        return

    if log_dir:
        setup_logging(log_dir, "MaintenanceOrchestrator")

    log_and_print("=" * 60)
    log_and_print("SDE MAINTENANCE ORCHESTRATOR")
    log_and_print("=" * 60)
    log_and_print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    results = run_maintenance_sequence()

    summary = format_summary(results)
    print(summary)

    if log_dir:
        timestr = time.strftime("%Y-%m-%d_%H%M%S")
        report_path = os.path.join(log_dir, f"{timestr}_maintenance_report.txt")
        with open(report_path, 'w') as f:
            f.write(summary)
        log_and_print(f"\nFull report saved: {report_path}")

    if results['success']:
        log_and_print("\nMaintenance completed successfully!")
    else:
        log_and_print("\nMaintenance completed with errors", "error")

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
