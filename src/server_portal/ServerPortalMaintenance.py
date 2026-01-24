"""ArcGIS Server maintenance operations.

Clear map service caches, check service health, and manage services.
"""

import json
import os
import sys
import time
import urllib3

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sde_utils import setup_logging, log_and_print, get_ags_token

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()


def list_services(server_url, token, folder=None):
    """List all map services on server.

    Args:
        server_url: ArcGIS Server admin URL
        token: Authentication token
        folder: Specific folder to list (optional)

    Returns:
        List of service dicts
    """
    services = []

    if folder:
        url = f"{server_url}/admin/services/{folder}"
    else:
        url = f"{server_url}/admin/services"

    params = {'token': token, 'f': 'json'}

    try:
        response = requests.get(url, params=params, timeout=30, verify=False)
        data = response.json()

        for svc in data.get('services', []):
            services.append({
                'name': svc['serviceName'],
                'type': svc['type'],
                'folder': folder or 'root'
            })

        if not folder:
            for subfolder in data.get('folders', []):
                services.extend(list_services(server_url, token, subfolder))

    except Exception as e:
        log_and_print(f"Error listing services: {e}", "error")

    return services


def get_service_status(server_url, service_name, service_type, token, folder=None):
    """Get service status and statistics.

    Args:
        server_url: ArcGIS Server admin URL
        service_name: Name of service
        service_type: Type (MapServer, FeatureServer, etc.)
        token: Authentication token
        folder: Service folder (optional)

    Returns:
        Dict with service status
    """
    if folder and folder != 'root':
        url = f"{server_url}/admin/services/{folder}/{service_name}.{service_type}/status"
    else:
        url = f"{server_url}/admin/services/{service_name}.{service_type}/status"

    params = {'token': token, 'f': 'json'}

    try:
        response = requests.get(url, params=params, timeout=30, verify=False)
        data = response.json()

        return {
            'name': service_name,
            'type': service_type,
            'status': data.get('realTimeState', 'UNKNOWN'),
            'configured_state': data.get('configuredState', 'UNKNOWN')
        }
    except Exception as e:
        return {
            'name': service_name,
            'type': service_type,
            'status': 'ERROR',
            'error': str(e)
        }


def clear_service_cache(server_url, service_name, service_type, token, folder=None):
    """Clear cache for a map service.

    Args:
        server_url: ArcGIS Server admin URL
        service_name: Name of service
        service_type: Type (MapServer)
        token: Authentication token
        folder: Service folder (optional)

    Returns:
        True if successful, False otherwise
    """
    if service_type != 'MapServer':
        return False

    if folder and folder != 'root':
        url = f"{server_url}/admin/services/{folder}/{service_name}.{service_type}/deleteTiles"
    else:
        url = f"{server_url}/admin/services/{service_name}.{service_type}/deleteTiles"

    params = {
        'token': token,
        'f': 'json',
        'numOfCachingServiceInstances': 2
    }

    try:
        response = requests.post(url, data=params, timeout=60, verify=False)
        data = response.json()

        if 'error' in data:
            log_and_print(f"Error clearing cache for {service_name}: {data['error']}", "error")
            return False

        return data.get('success', False)
    except Exception as e:
        log_and_print(f"Error clearing cache: {e}", "error")
        return False


def restart_service(server_url, service_name, service_type, token, folder=None):
    """Stop and start a service.

    Args:
        server_url: ArcGIS Server admin URL
        service_name: Name of service
        service_type: Type
        token: Authentication token
        folder: Service folder (optional)

    Returns:
        True if successful, False otherwise
    """
    if folder and folder != 'root':
        base_url = f"{server_url}/admin/services/{folder}/{service_name}.{service_type}"
    else:
        base_url = f"{server_url}/admin/services/{service_name}.{service_type}"

    params = {'token': token, 'f': 'json'}

    try:
        response = requests.post(f"{base_url}/stop", data=params, timeout=60, verify=False)
        if 'error' in response.json():
            return False

        time.sleep(5)

        response = requests.post(f"{base_url}/start", data=params, timeout=60, verify=False)
        if 'error' in response.json():
            return False

        return True
    except Exception as e:
        log_and_print(f"Error restarting service: {e}", "error")
        return False


def generate_health_report(server_url, token):
    """Generate health report for all services.

    Args:
        server_url: ArcGIS Server admin URL
        token: Authentication token

    Returns:
        Dict with health report
    """
    services = list_services(server_url, token)
    log_and_print(f"Found {len(services)} services")

    report = {
        'server_url': server_url,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
        'total_services': len(services),
        'started': 0,
        'stopped': 0,
        'error': 0,
        'services': []
    }

    for svc in services:
        status = get_service_status(
            server_url, svc['name'], svc['type'], token, svc['folder']
        )

        if status['status'] == 'STARTED':
            report['started'] += 1
        elif status['status'] == 'STOPPED':
            report['stopped'] += 1
        else:
            report['error'] += 1

        report['services'].append(status)

    return report


def format_report(report):
    """Format health report for display.

    Args:
        report: Health report dict

    Returns:
        Formatted string
    """
    lines = [
        f"ArcGIS Server Health Report",
        "=" * 60,
        f"Server: {report['server_url']}",
        f"Time: {report['timestamp']}",
        "",
        f"Total Services: {report['total_services']}",
        f"  Started: {report['started']}",
        f"  Stopped: {report['stopped']}",
        f"  Error: {report['error']}",
        ""
    ]

    if report['stopped'] > 0 or report['error'] > 0:
        lines.append("Services with Issues:")
        lines.append("-" * 40)
        for svc in report['services']:
            if svc['status'] != 'STARTED':
                lines.append(f"  {svc['folder']}/{svc['name']}.{svc['type']}: {svc['status']}")

    return "\n".join(lines)


def main():
    """Main entry point for server maintenance."""
    server_url = os.environ.get('AGS_SERVER_URL')
    username = os.environ.get('AGS_ADMIN_USER')
    password = os.environ.get('AGS_ADMIN_PASSWORD')
    log_dir = os.environ.get('SDE_LOG_DIR')
    clear_all = os.environ.get('CLEAR_ALL_CACHES', 'false').lower() == 'true'

    if not all([server_url, username, password]):
        print("Error: AGS_SERVER_URL, AGS_ADMIN_USER, and AGS_ADMIN_PASSWORD are required")
        return

    if log_dir:
        setup_logging(log_dir, "ServerPortalMaintenance")

    log_and_print(f"Connecting to: {server_url}")

    try:
        token = get_ags_token(server_url, username, password)
        log_and_print("Authentication successful")
    except Exception as e:
        log_and_print(f"Authentication failed: {e}", "error")
        return

    report = generate_health_report(server_url, token)
    print(format_report(report))

    if clear_all:
        log_and_print("\nClearing map service caches...")
        map_services = [s for s in report['services'] if s['type'] == 'MapServer']
        cleared = 0
        for svc in map_services:
            folder = svc.get('folder')
            if folder == 'root':
                folder = None
            if clear_service_cache(server_url, svc['name'], svc['type'], token, folder):
                log_and_print(f"  Cleared: {svc['name']}")
                cleared += 1
        log_and_print(f"Cleared {cleared}/{len(map_services)} map service caches")

    if log_dir:
        timestr = time.strftime("%Y-%m-%d_%H%M%S")
        json_path = os.path.join(log_dir, f"{timestr}_server_health.json")
        with open(json_path, 'w') as f:
            json.dump(report, f, indent=2)
        log_and_print(f"\nReport saved: {json_path}")

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
