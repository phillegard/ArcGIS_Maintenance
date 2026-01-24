"""Disconnect all users before maintenance operations.

Forces disconnection of user sessions from SDE geodatabases
to ensure clean maintenance windows.
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


def get_connected_users(database_path):
    """Get list of currently connected users.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of user connection objects
    """
    try:
        users = arcpy.ListUsers(database_path)
        return users if users else []
    except arcpy.ExecuteError as e:
        log_and_print(f"Error listing users: {e}", "error")
        return []


def format_user_info(user):
    """Format user connection information for display.

    Args:
        user: ArcPy user connection object

    Returns:
        Formatted string with user details
    """
    return f"ID={user.ID}, Name={user.Name}, Machine={user.ClientName}"


def disconnect_user(database_path, user_id):
    """Disconnect a specific user by ID.

    Args:
        database_path: Path to .sde connection file
        user_id: Connection ID to disconnect

    Returns:
        True if successful, False otherwise
    """
    try:
        arcpy.DisconnectUser(database_path, user_id)
        return True
    except arcpy.ExecuteError as e:
        log_and_print(f"Error disconnecting user {user_id}: {e}", "error")
        return False


def disconnect_all(database_path, exclude_admin=True):
    """Disconnect all users from database.

    Args:
        database_path: Path to .sde connection file
        exclude_admin: Whether to exclude admin/sde users

    Returns:
        Dict with counts of disconnected and failed
    """
    users = get_connected_users(database_path)
    if not users:
        return {'disconnected': 0, 'failed': 0, 'excluded': 0}

    disconnected = 0
    failed = 0
    excluded = 0

    for user in users:
        if exclude_admin:
            if user.Name and user.Name.lower() in ['sde', 'admin', 'dbo']:
                log_and_print(f"Excluding admin user: {format_user_info(user)}")
                excluded += 1
                continue

        log_and_print(f"Disconnecting: {format_user_info(user)}")
        if disconnect_user(database_path, user.ID):
            disconnected += 1
        else:
            failed += 1

    return {'disconnected': disconnected, 'failed': failed, 'excluded': excluded}


def wait_for_disconnect(database_path, timeout_seconds=60):
    """Wait until all users are disconnected or timeout.

    Args:
        database_path: Path to .sde connection file
        timeout_seconds: Maximum wait time

    Returns:
        True if all disconnected, False if timeout
    """
    check_interval = 5
    elapsed = 0

    while elapsed < timeout_seconds:
        users = get_connected_users(database_path)
        non_admin = [u for u in users if u.Name.lower() not in ['sde', 'admin', 'dbo']]

        if not non_admin:
            return True

        log_and_print(f"Waiting... {len(non_admin)} user(s) still connected")
        time.sleep(check_interval)
        elapsed += check_interval

    return False


def process_database(database_path, sde_name, exclude_admin, timeout_seconds):
    """Disconnect users for a single database.

    Args:
        database_path: Path to .sde connection file
        sde_name: Name of database for logging
        exclude_admin: Whether to exclude admin users
        timeout_seconds: Timeout for waiting

    Returns:
        Dict with processing summary
    """
    log_and_print(f"Disconnecting users from: {sde_name}")

    users = get_connected_users(database_path)
    log_and_print(f"Found {len(users)} connected user(s)")

    if not users:
        return {'database': sde_name, 'disconnected': 0, 'failed': 0}

    for user in users:
        log_and_print(f"  {format_user_info(user)}")

    result = disconnect_all(database_path, exclude_admin)
    log_and_print(f"Disconnected: {result['disconnected']}, Failed: {result['failed']}, Excluded: {result['excluded']}")

    if result['disconnected'] > 0:
        log_and_print("Waiting for disconnections to complete...")
        if wait_for_disconnect(database_path, timeout_seconds):
            log_and_print("All users disconnected")
        else:
            log_and_print("Timeout waiting for disconnections", "warning")

    return {
        'database': sde_name,
        'total_users': len(users),
        **result
    }


def main():
    """Main entry point for user disconnection."""
    connection_dir = os.environ.get('SDE_CONNECTION_DIR')
    log_dir = os.environ.get('SDE_LOG_DIR')
    timeout = int(os.environ.get('DISCONNECT_TIMEOUT_SECONDS', '60'))
    exclude_admin = os.environ.get('EXCLUDE_ADMIN_USER', 'true').lower() == 'true'

    validate_paths(connection_dir=connection_dir, log_dir=log_dir)
    setup_logging(log_dir, "DisconnectUsers")

    workspace = arcpy.GetParameterAsText(0)
    if workspace:
        arcpy.env.workspace = workspace

    sde_files = get_sde_connections(connection_dir)
    if not sde_files:
        log_and_print(f"No .sde files found in {connection_dir}", "warning")
        return

    for sde_path in sde_files:
        sde_name = os.path.basename(sde_path)
        process_database(sde_path, sde_name, exclude_admin, timeout)

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
