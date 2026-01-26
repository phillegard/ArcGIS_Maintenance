"""Audit Portal items for sharing compliance.

Identifies items shared with the organization or publicly when they should be private.
Generates reports listing non-compliant items by access level.
"""

import os
import sys
import time

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sde_utils import setup_logging, log_and_print, get_portal_token

load_dotenv()


def get_org_id(portal_url, token):
    """Get the organization ID from Portal self endpoint.

    Args:
        portal_url: Portal base URL
        token: Authentication token

    Returns:
        Organization ID string
    """
    self_url = f"{portal_url}/sharing/rest/portals/self"
    params = {'f': 'json', 'token': token}
    response = requests.get(self_url, params=params, timeout=60)
    data = response.json()
    return data.get('id', '')


def query_all_portal_items(portal_url, token):
    """Query all items in Portal regardless of type.

    Args:
        portal_url: Portal base URL
        token: Authentication token

    Returns:
        List of item dicts with id, title, owner, type, access fields
    """
    search_url = f"{portal_url}/sharing/rest/search"

    # Get org ID - wildcard search doesn't always work on Portal
    org_id = get_org_id(portal_url, token)
    query = f'orgid:{org_id}' if org_id else '*'

    params = {
        'q': query,
        'num': 100,
        'start': 1,
        'f': 'json',
        'token': token
    }

    items = []
    try:
        while True:
            response = requests.get(search_url, params=params, timeout=60)
            data = response.json()

            if 'error' in data:
                log_and_print(f"Search error: {data['error']}", "error")
                break

            for item in data.get('results', []):
                # Skip Esri system accounts
                if item['owner'] in ('esri_nav', 'esri_apps'):
                    continue
                items.append({
                    'id': item['id'],
                    'title': item['title'],
                    'owner': item['owner'],
                    'type': item['type'],
                    'access': item.get('access', 'private'),
                    'created': item.get('created'),
                    'modified': item.get('modified'),
                    'url': item.get('url', '')
                })

            if data.get('nextStart', -1) == -1:
                break
            params['start'] = data['nextStart']

    except Exception as e:
        log_and_print(f"Error querying items: {e}", "error")

    return items


def categorize_by_access(items):
    """Categorize items by their sharing level.

    Args:
        items: List of item dicts

    Returns:
        Dict with 'public', 'org', 'shared', 'private' lists
    """
    categories = {
        'public': [],
        'org': [],
        'shared': [],
        'private': []
    }

    for item in items:
        access = item.get('access', 'private').lower()
        if access == 'public':
            categories['public'].append(item)
        elif access == 'org':
            categories['org'].append(item)
        elif access == 'shared':
            categories['shared'].append(item)
        else:
            categories['private'].append(item)

    return categories


def generate_summary(categories, items):
    """Generate summary statistics.

    Args:
        categories: Dict from categorize_by_access
        items: Original items list

    Returns:
        Dict with counts and percentages
    """
    total = len(items)
    return {
        'total_items': total,
        'public_count': len(categories['public']),
        'org_count': len(categories['org']),
        'shared_count': len(categories['shared']),
        'private_count': len(categories['private']),
        'non_compliant_count': len(categories['public']) + len(categories['org']),
        'public_percent': round(len(categories['public']) / total * 100, 1) if total > 0 else 0,
        'org_percent': round(len(categories['org']) / total * 100, 1) if total > 0 else 0,
        'shared_percent': round(len(categories['shared']) / total * 100, 1) if total > 0 else 0,
        'private_percent': round(len(categories['private']) / total * 100, 1) if total > 0 else 0
    }


def format_text_report(summary, categories, portal_url):
    """Format sharing audit report for display.

    Args:
        summary: Summary statistics dict
        categories: Categorized items
        portal_url: Portal URL for report header

    Returns:
        Formatted string
    """
    lines = [
        "Portal Sharing Audit Report",
        "=" * 70,
        f"Portal: {portal_url}",
        f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "SUMMARY",
        "-" * 40,
        f"Total Items:         {summary['total_items']:>6}",
        f"Public Items:        {summary['public_count']:>6}  ({summary['public_percent']}%)",
        f"Organization Items:  {summary['org_count']:>6}  ({summary['org_percent']}%)",
        f"Group Shared Items:  {summary['shared_count']:>6}  ({summary['shared_percent']}%)",
        f"Private Items:       {summary['private_count']:>6}  ({summary['private_percent']}%)",
        "",
        f"Non-compliant Total: {summary['non_compliant_count']:>6}",
        ""
    ]

    if categories['public']:
        lines.append("PUBLIC ITEMS (shared with everyone)")
        lines.append("-" * 70)
        lines.append(f"{'Title':<35} {'Owner':<15} {'Type':<20}")
        lines.append("-" * 70)
        for item in categories['public']:
            title = item['title'][:35] if len(item['title']) > 35 else item['title']
            owner = item['owner'][:15] if len(item['owner']) > 15 else item['owner']
            item_type = item['type'][:20] if len(item['type']) > 20 else item['type']
            lines.append(f"{title:<35} {owner:<15} {item_type:<20}")
            lines.append(f"  ID: {item['id']}")
        lines.append("")

    if categories['org']:
        lines.append("ORGANIZATION ITEMS (shared with organization)")
        lines.append("-" * 70)
        lines.append(f"{'Title':<35} {'Owner':<15} {'Type':<20}")
        lines.append("-" * 70)
        for item in categories['org']:
            title = item['title'][:35] if len(item['title']) > 35 else item['title']
            owner = item['owner'][:15] if len(item['owner']) > 15 else item['owner']
            item_type = item['type'][:20] if len(item['type']) > 20 else item['type']
            lines.append(f"{title:<35} {owner:<15} {item_type:<20}")
            lines.append(f"  ID: {item['id']}")
        lines.append("")

    if summary['non_compliant_count'] == 0:
        lines.append("All items are private - no sharing concerns found.")

    return "\n".join(lines)


def save_report(text_report, output_dir, portal_name):
    """Save text report.

    Args:
        text_report: Formatted text report string
        output_dir: Output directory
        portal_name: Portal name for filename
    """
    timestr = time.strftime("%Y-%m-%d_%H%M%S")
    safe_name = "".join(c if c.isalnum() or c in '._-' else '_' for c in portal_name)

    txt_path = os.path.join(output_dir, f"{timestr}_{safe_name}_sharing_audit.txt")
    with open(txt_path, 'w') as f:
        f.write(text_report)
    log_and_print(f"Report saved: {txt_path}")


def main():
    """Main entry point for Portal sharing audit."""
    portal_url = os.environ.get('PORTAL_URL')
    username = os.environ.get('PORTAL_ADMIN_USER')
    password = os.environ.get('PORTAL_ADMIN_PASSWORD')
    log_dir = os.environ.get('SDE_LOG_DIR')

    if not all([portal_url, username, password]):
        print("Error: PORTAL_URL, PORTAL_ADMIN_USER, and PORTAL_ADMIN_PASSWORD required")
        return

    if log_dir:
        setup_logging(log_dir, "PortalSharingAudit")

    log_and_print(f"Portal: {portal_url}")
    log_and_print("Starting sharing audit...")

    try:
        token = get_portal_token(portal_url, username, password)
        log_and_print("Authentication successful")
    except Exception as e:
        log_and_print(f"Authentication failed: {e}", "error")
        return

    items = query_all_portal_items(portal_url, token)
    log_and_print(f"Found {len(items)} total items")

    if not items:
        log_and_print("No items found in Portal")
        return

    categories = categorize_by_access(items)
    summary = generate_summary(categories, items)

    text_report = format_text_report(summary, categories, portal_url)
    print(text_report)

    if log_dir:
        portal_name = portal_url.replace('https://', '').replace('http://', '').split('/')[0]
        save_report(text_report, log_dir, portal_name)

    if summary['non_compliant_count'] == 0:
        log_and_print("All items are private - no sharing concerns found")
    else:
        log_and_print(f"Found {summary['non_compliant_count']} items shared publicly or with organization")

    log_and_print("DONE!")


if __name__ == "__main__":
    main()
