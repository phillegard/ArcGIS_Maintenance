"""Block or allow user connections for maintenance windows.

Controls the AcceptConnections property on SDE geodatabases
to prevent new connections during maintenance operations.
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


def set_accept_connections(database_path, accept):
    """Enable or disable new connections to geodatabase.

    Args:
        database_path: Path to .sde connection file
        accept: True to allow connections, False to block

    Returns:
        True if successful, False otherwise
    """
    try:
        arcpy.AcceptConnections(database_path, accept)
        return True
    except arcpy.ExecuteError as e:
        log_and_print(f"Error setting connections: {e}", "error")
        return False


def get_connection_count(database_path):
    """Get current number of connected users.

    Args:
        database_path: Path to .sde connection file

    Returns:
        Number of connected users
    """
    try:
        users = arcpy.ListUsers(database_path)
        return len(users) if users else 0
    except arcpy.ExecuteError:
        return -1


def block_and_wait(database_path, timeout_minutes=5):
    """Block connections and wait for existing connections to drain.

    Args:
        database_path: Path to .sde connection file
        timeout_minutes: Maximum time to wait for connections to drain

    Returns:
        Dict with success status and final connection count
    """
    if not set_accept_connections(database_path, False):
        return {'success': False, 'connections_remaining': -1}

    log_and_print("Connections blocked. Waiting for existing connections to drain...")

    timeout_seconds = timeout_minutes * 60
    check_interval = 10
    elapsed = 0

    while elapsed < timeout_seconds:
        count = get_connection_count(database_path)
        if count == 0:
            log_and_print("All connections drained")
            return {'success': True, 'connections_remaining': 0}
        if count == 1:
            log_and_print("Only admin connection remains")
            return {'success': True, 'connections_remaining': 1}

        log_and_print(f"Waiting... {count} connection(s) remaining")
        time.sleep(check_interval)
        elapsed += check_interval

    final_count = get_connection_count(database_path)
    log_and_print(f"Timeout reached. {final_count} connection(s) still active", "warning")

    return {'success': False, 'connections_remaining': final_count}


def process_database(database_path, sde_name, action, timeout_minutes):
    """Manage connections for a single database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        action: "block", "allow", or "block_and_wait"
        timeout_minutes: Timeout for block_and_wait

    Returns:
        Dict with action result
    """
    log_and_print(f"Managing connections for: {sde_name}")

    if action == "allow":
        success = set_accept_connections(database_path, True)
        status = "enabled" if success else "failed"
        log_and_print(f"Connections {status}")
        return {'database': sde_name, 'action': action, 'success': success}

    elif action == "block":
        success = set_accept_connections(database_path, False)
        status = "blocked" if success else "failed"
        log_and_print(f"Connections {status}")
        return {'database': sde_name, 'action': action, 'success': success}

    elif action == "block_and_wait":
        result = block_and_wait(database_path, timeout_minutes)
        return {
            'database': sde_name,
            'action': action,
            'success': result['success'],
            'connections_remaining': result['connections_remaining']
        }

    else:
        log_and_print(f"Unknown action: {action}", "error")
        return {'database': sde_name, 'action': action, 'success': False}


def main():
    """Main entry point for connection management."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    timeout_minutes = int(os.environ.get('CONNECTION_TIMEOUT_MINUTES', '5'))

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "ManageConnections")

    action = arcpy.GetParameterAsText(0) or "block"
    workspace = arcpy.GetParameterAsText(1)
    if workspace:
        arcpy.env.workspace = workspace

    log_and_print(f"Action: {action}")

    sde_files = get_sde_connections(connection_dir)
    if not sde_files:
        log_and_print(f"No .sde files found in {connection_dir}", "warning")
        return

    for sde_path in sde_files:
        sde_name = os.path.basename(sde_path)
        process_database(sde_path, sde_name, action, timeout_minutes)

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
