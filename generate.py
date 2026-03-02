#!/usr/bin/env python3
"""
generate.py — Regenera docs/index.html con datos frescos de BigQuery.

Uso local:    python generate.py
En CI/CD:    idem (auth via GOOGLE_APPLICATION_CREDENTIALS o Workload Identity)
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

SITES = ['MLA', 'MLB', 'MLM', 'MLC', 'MLU', 'MCO', 'MPE']
MAX_ROWS_PER_CLUSTER = 200
SQL = Path('main.sql').read_text(encoding='utf-8')


def query_site(client: bigquery.Client, site_id: str) -> list[dict]:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('site_id', 'STRING', site_id),
            bigquery.ScalarQueryParameter('ros_filter', 'STRING', 'all'),
            bigquery.ScalarQueryParameter('time_window', 'STRING', 'today'),
            bigquery.ScalarQueryParameter('max_rows_per_cluster', 'INT64', MAX_ROWS_PER_CLUSTER),
        ]
    )
    rows = list(client.query(SQL, job_config=job_config).result())
    return [
        {
            'site': site_id,
            'c': row.priority_cluster,
            'li': row.line_item_id,
            'camp': row.campaign_id,
            'impr': row.impressions,
            'clk': row.clicks,
            'ctr': float(row.ctr) if row.ctr is not None else None,
            'owner': str(row.owner) if row.owner else '',
            'end': str(row.end_date),
            'ros': bool(row.is_ros),
        }
        for row in rows
    ]


def main():
    client = bigquery.Client()
    all_data = []

    for site in SITES:
        print(f'Querying {site}...', flush=True)
        try:
            rows = query_site(client, site)
            all_data.extend(rows)
            print(f'  → {len(rows)} filas', flush=True)
        except Exception as e:
            print(f'  ERROR en {site}: {e}', file=sys.stderr)
            sys.exit(1)

    template = Path('dashboard.html').read_text(encoding='utf-8')

    # Reemplaza el bloque const DATA=[...];
    data_json = json.dumps(all_data, ensure_ascii=False, separators=(',', ':'))
    html, n = re.subn(
        r'const DATA=\[.*?\];',
        f'const DATA={data_json};',
        template,
        flags=re.DOTALL,
    )
    if n == 0:
        print('ERROR: no se encontró const DATA=[...]; en dashboard.html', file=sys.stderr)
        sys.exit(1)

    # Actualiza el badge de fecha
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    html = re.sub(
        r'(Datos: <strong>)[^<]+(</strong>)',
        rf'\g<1>{now}\g<2>',
        html,
    )

    docs = Path('docs')
    docs.mkdir(exist_ok=True)
    (docs / 'index.html').write_text(html, encoding='utf-8')

    print(f'\nListo: docs/index.html ({len(all_data)} registros, generado {now})')


if __name__ == '__main__':
    main()
