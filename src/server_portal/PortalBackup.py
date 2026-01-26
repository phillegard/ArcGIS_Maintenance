"""Export hosted feature services from Portal for backup.

Backs up Portal hosted feature services to local geodatabases.
These are services that live in Portal's managed datastore, not in your SQL Server.
"""

import os
import sys
import time
import zipfile

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sde_utils import setup_logging, log_and_print, get_portal_token

load_dotenv()


def list_hosted_feature_services(portal_url, token, owner=None):
    """List all hosted feature services.

    Args:
        portal_url: Portal base URL
        token: Authentication token
        owner: Filter by owner username (optional)

    Returns:
        List of service item dicts
    """
    search_url = f"{portal_url}/sharing/rest/search"

    query = 'type:"Feature Service" AND typekeywords:Hosted'
    if owner:
        query += f' AND owner:{owner}'

    params = {
        'q': query,
        'num': 100,
        'start': 1,
        'f': 'json',
        'token': token
    }

    services = []
    try:
        while True:
            response = requests.get(search_url, params=params, timeout=60)
            data = response.json()

            for item in data.get('results', []):
                services.append({
                    'id': item['id'],
                    'title': item['title'],
                    'owner': item['owner'],
                    'url': item.get('url', ''),
                    'created': item.get('created'),
                    'modified': item.get('modified')
                })

            if data.get('nextStart', -1) == -1:
                break
            params['start'] = data['nextStart']

    except Exception as e:
        log_and_print(f"Error listing services: {e}", "error")

    return services


def export_feature_service(portal_url, item_id, owner, token, output_format="File Geodatabase"):
    """Export a hosted feature service.

    Args:
        portal_url: Portal base URL
        item_id: Item ID of the service
        owner: Owner username
        token: Authentication token
        output_format: Export format (File Geodatabase, Shapefile, GeoJSON, etc.)

    Returns:
        Export job info or None on failure
    """
    export_url = f"{portal_url}/sharing/rest/content/users/{owner}/export"

    params = {
        'itemId': item_id,
        'exportFormat': output_format,
        'f': 'json',
        'token': token
    }

    try:
        response = requests.post(export_url, data=params, timeout=120)
        data = response.json()

        if 'error' in data:
            log_and_print(f"Export error: {data['error']['message']}", "error")
            return None

        return {
            'export_item_id': data.get('exportItemId'),
            'job_id': data.get('jobId'),
            'service_item_id': item_id
        }
    except Exception as e:
        log_and_print(f"Export request failed: {e}", "error")
        return None


def check_export_status(portal_url, job_id, export_item_id, token, max_wait=600):
    """Wait for export job to complete.

    Args:
        portal_url: Portal base URL
        job_id: Export job ID
        export_item_id: ID of export item
        token: Authentication token
        max_wait: Maximum wait time in seconds

    Returns:
        True if completed, False if failed/timeout
    """
    status_url = f"{portal_url}/sharing/rest/content/items/{export_item_id}"

    params = {'f': 'json', 'token': token}
    start_time = time.time()

    while time.time() - start_time < max_wait:
        try:
            response = requests.get(status_url, params=params, timeout=30)
            data = response.json()

            if 'error' not in data:
                return True

            time.sleep(10)

        except Exception:
            time.sleep(10)

    return False


def download_export(portal_url, export_item_id, token, output_dir, filename):
    """Download exported item to local directory.

    Args:
        portal_url: Portal base URL
        export_item_id: ID of export item
        token: Authentication token
        output_dir: Local output directory
        filename: Output filename

    Returns:
        Path to downloaded file or None
    """
    data_url = f"{portal_url}/sharing/rest/content/items/{export_item_id}/data"

    params = {'token': token}

    try:
        response = requests.get(data_url, params=params, timeout=300, stream=True)

        if response.status_code != 200:
            log_and_print(f"Download failed: HTTP {response.status_code}", "error")
            return None

        safe_filename = "".join(c if c.isalnum() or c in '._-' else '_' for c in filename)
        output_path = os.path.join(output_dir, safe_filename)

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return output_path

    except Exception as e:
        log_and_print(f"Download error: {e}", "error")
        return None


def delete_export_item(portal_url, export_item_id, owner, token):
    """Delete temporary export item from Portal.

    Args:
        portal_url: Portal base URL
        export_item_id: ID of export item
        owner: Owner username
        token: Authentication token

    Returns:
        True if deleted, False otherwise
    """
    delete_url = f"{portal_url}/sharing/rest/content/users/{owner}/items/{export_item_id}/delete"

    params = {'f': 'json', 'token': token}

    try:
        response = requests.post(delete_url, data=params, timeout=30)
        data = response.json()
        return data.get('success', False)
    except Exception:
        return False


def format_report(portal_url, results):
    """Format backup results as text report.

    Args:
        portal_url: Portal URL
        results: List of backup result dicts

    Returns:
        Formatted report string
    """
    lines = [
        "Portal Backup Report",
        "=" * 60,
        f"Portal: {portal_url}",
        f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"Total Services: {len(results)}",
        f"Successful: {sum(1 for r in results if r['success'])}",
        f"Failed: {sum(1 for r in results if not r['success'])}",
        ""
    ]

    successful = [r for r in results if r['success']]
    if successful:
        lines.append("Successful Backups:")
        lines.append("-" * 40)
        for r in successful:
            lines.append(f"  {r['service']}")
            if r.get('path'):
                lines.append(f"    -> {r['path']}")
        lines.append("")

    failed = [r for r in results if not r['success']]
    if failed:
        lines.append("Failed Backups:")
        lines.append("-" * 40)
        for r in failed:
            lines.append(f"  {r['service']} (ID: {r['id']})")
        lines.append("")

    return "\n".join(lines)


def backup_service(portal_url, service, token, output_dir, output_format, owner):
    """Backup a single hosted feature service.

    Args:
        portal_url: Portal base URL
        service: Service info dict
        token: Authentication token
        output_dir: Output directory
        output_format: Export format
        owner: Portal username for export

    Returns:
        Dict with backup result
    """
    result = {
        'service': service['title'],
        'id': service['id'],
        'success': False,
        'path': None
    }

    log_and_print(f"  Exporting: {service['title']}")

    export_info = export_feature_service(
        portal_url, service['id'], owner, token, output_format
    )

    if not export_info:
        return result

    log_and_print(f"    Waiting for export to complete...")

    if not check_export_status(portal_url, export_info.get('job_id'),
                                export_info['export_item_id'], token):
        log_and_print(f"    Export timeout or failed", "error")
        return result

    extension = '.gdb.zip' if 'Geodatabase' in output_format else '.zip'
    timestr = time.strftime("%Y%m%d")
    filename = f"{timestr}_{service['title']}{extension}"

    log_and_print(f"    Downloading...")
    download_path = download_export(
        portal_url, export_info['export_item_id'], token, output_dir, filename
    )

    if download_path:
        result['success'] = True
        result['path'] = download_path
        log_and_print(f"    Saved: {download_path}")

        delete_export_item(portal_url, export_info['export_item_id'], owner, token)
    else:
        log_and_print(f"    Download failed", "error")

    return result


def main():
    """Main entry point for Portal backup."""
    portal_url = os.environ.get('PORTAL_URL')
    username = os.environ.get('PORTAL_ADMIN_USER')
    password = os.environ.get('PORTAL_ADMIN_PASSWORD')
    backup_dir = os.environ.get('PORTAL_BACKUP_DIR')
    output_format = os.environ.get('PORTAL_BACKUP_FORMAT', 'File Geodatabase')
    owner_filter = os.environ.get('PORTAL_BACKUP_OWNER_FILTER', '')
    log_dir = os.environ.get('SDE_LOG_DIR')

    if not all([portal_url, username, password, backup_dir]):
        print("Error: PORTAL_URL, PORTAL_ADMIN_USER, PORTAL_ADMIN_PASSWORD, and PORTAL_BACKUP_DIR are required")
        return

    if not os.path.isdir(backup_dir):
        os.makedirs(backup_dir, exist_ok=True)

    if log_dir:
        setup_logging(log_dir, "PortalBackup")

    log_and_print(f"Portal: {portal_url}")
    log_and_print(f"Backup directory: {backup_dir}")
    log_and_print(f"Format: {output_format}")

    try:
        token = get_portal_token(portal_url, username, password)
        log_and_print("Authentication successful")
    except Exception as e:
        log_and_print(f"Authentication failed: {e}", "error")
        return

    services = list_hosted_feature_services(portal_url, token, owner_filter or None)
    log_and_print(f"Found {len(services)} hosted feature service(s)")

    if not services:
        log_and_print("No services to backup")
        return

    results = []
    for service in services:
        result = backup_service(
            portal_url, service, token, backup_dir, output_format, username
        )
        results.append(result)

    success_count = sum(1 for r in results if r['success'])
    log_and_print(f"\nBackup complete: {success_count}/{len(services)} successful")

    if log_dir:
        timestr = time.strftime("%Y-%m-%d_%H%M%S")
        report_path = os.path.join(log_dir, f"{timestr}_portal_backup.txt")
        with open(report_path, 'w') as f:
            f.write(format_report(portal_url, results))
        log_and_print(f"Report saved: {report_path}")

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
