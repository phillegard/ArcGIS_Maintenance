"""Shared utilities for ArcGIS SDE maintenance scripts.

Common functions for logging, path validation, SQL execution, and API authentication.
"""

import logging
import os
import time

import arcpy
import requests
from dotenv import load_dotenv

load_dotenv()


def setup_logging(log_dir, script_name):
    """Configure logging with timestamped filename for specific script.

    Args:
        log_dir: Directory for log files
        script_name: Name of the script (used in log filename)
    """
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    timestr = time.strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{timestr}_{script_name}.txt")

    logging.basicConfig(
        filename=log_file,
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO
    )


def log_and_print(message, level="info"):
    """Dual output to logging and console.

    Args:
        message: Message to log
        level: Log level (info, warning, error)
    """
    print(message)
    log_func = getattr(logging, level, logging.info)
    log_func(message)


def validate_paths(**paths):
    """Validate that required directories/files exist.

    Args:
        **paths: Named paths to validate (name=path)

    Raises:
        ValueError: If any path is missing or invalid
    """
    for name, path in paths.items():
        if not path:
            raise ValueError(f"{name} environment variable is not set")
        if name.endswith("_dir") and not os.path.isdir(path):
            raise ValueError(f"Directory not found: {path}")


def get_sde_connections(connection_dir):
    """Return list of .sde file paths from directory.

    Args:
        connection_dir: Directory containing .sde files

    Returns:
        List of full paths to .sde files
    """
    sde_files = [f for f in os.listdir(connection_dir) if f.endswith('.sde')]
    return [os.path.join(connection_dir, sde) for sde in sde_files]


def get_admin_connection(connection_dir, admin_suffix="_admin"):
    """Get admin-level SDE connection file.

    Args:
        connection_dir: Directory containing .sde files
        admin_suffix: Suffix identifying admin connections (default: _admin)

    Returns:
        Path to admin connection file, or None if not found
    """
    for f in os.listdir(connection_dir):
        if f.endswith('.sde') and admin_suffix in f.lower():
            return os.path.join(connection_dir, f)
    return None


def execute_sql(database_path, sql):
    """Execute SQL via arcpy.ArcSDESQLExecute and return results.

    Args:
        database_path: Path to .sde connection file
        sql: SQL query to execute

    Returns:
        Query results (list of rows or single value)
    """
    sde_conn = arcpy.ArcSDESQLExecute(database_path)
    try:
        result = sde_conn.execute(sql)
        return result
    finally:
        del sde_conn


def get_portal_token(portal_url, username, password):
    """Authenticate to Portal and return token.

    Args:
        portal_url: Base Portal URL (e.g., https://portal.domain.com/arcgis)
        username: Portal admin username
        password: Portal admin password

    Returns:
        Authentication token string

    Raises:
        requests.RequestException: If authentication fails
    """
    token_url = f"{portal_url}/sharing/rest/generateToken"
    params = {
        'username': username,
        'password': password,
        'client': 'referer',
        'referer': portal_url,
        'f': 'json'
    }
    response = requests.post(token_url, data=params, timeout=30)
    response.raise_for_status()

    result = response.json()
    if 'error' in result:
        raise ValueError(f"Portal auth failed: {result['error']['message']}")

    return result['token']


def get_ags_token(server_url, username, password):
    """Authenticate to ArcGIS Server and return token.

    Args:
        server_url: ArcGIS Server admin URL (e.g., https://server:6443/arcgis)
        username: Server admin username
        password: Server admin password

    Returns:
        Authentication token string

    Raises:
        requests.RequestException: If authentication fails
    """
    token_url = f"{server_url}/admin/generateToken"
    params = {
        'username': username,
        'password': password,
        'client': 'requestip',
        'f': 'json'
    }
    response = requests.post(token_url, data=params, timeout=30, verify=False)
    response.raise_for_status()

    result = response.json()
    if 'error' in result:
        raise ValueError(f"Server auth failed: {result['error']['message']}")

    return result['token']


def format_bytes(size_bytes):
    """Format bytes to human-readable string.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def get_data_list(database_path):
    """Build list of all tables, feature classes, rasters, and datasets in a database.

    Args:
        database_path: Path to .sde connection file

    Returns:
        List of dataset names
    """
    arcpy.env.workspace = database_path
    data_list = arcpy.ListTables() + arcpy.ListFeatureClasses() + arcpy.ListRasters()

    for dataset in arcpy.ListDatasets("", "Feature"):
        arcpy.env.workspace = os.path.join(database_path, dataset)
        data_list += arcpy.ListFeatureClasses() + arcpy.ListDatasets()

    return data_list


def process_with_error_handling(operation_name, operation_func, *args, **kwargs):
    """Execute an operation with standardized error handling.

    Args:
        operation_name: Name of the operation for logging
        operation_func: Function to execute
        *args: Arguments to pass to function
        **kwargs: Keyword arguments to pass to function

    Returns:
        Tuple of (success: bool, result or error message)
    """
    try:
        result = operation_func(*args, **kwargs)
        return True, result
    except arcpy.ExecuteError as e:
        error_msg = f"ArcPy error during {operation_name}: {e}"
        logging.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error during {operation_name}: {e}"
        logging.error(error_msg)
        return False, error_msg
