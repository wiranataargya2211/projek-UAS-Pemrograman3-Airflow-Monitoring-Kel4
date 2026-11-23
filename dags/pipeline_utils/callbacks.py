import csv
import json
import logging
import os
import time
import urllib.request
from datetime import datetime

from pipeline_utils.config import (ALERT_LOG, EVENTS_FILE, MONITORING_DIR,
                                   PROCESSED_DIR, RAW_DIR)

log = logging.getLogger(__name__)


def _write_event(context, state):
    ti = context['task_instance']
    error = context.get('exception')
    MONITORING_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not EVENTS_FILE.exists()
    with open(EVENTS_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(['timestamp', 'dag_id', 'task_id', 'run_id',
                             'state', 'try_number', 'error'])
        writer.writerow([
            datetime.now().isoformat(timespec='seconds'),
            ti.dag_id, ti.task_id, context.get('run_id', ''), state,
            ti.try_number, str(error)[:200] if error else '',
        ])


def write_alert(message, send_webhook=False):
    MONITORING_DIR.mkdir(parents=True, exist_ok=True)
    line = datetime.now().isoformat(timespec='seconds') + ' | ' + message
    with open(ALERT_LOG, 'a', encoding='utf-8') as f:
        print(line, file=f)
    log.warning('ALERT: %s', message)
    url = os.environ.get('ALERT_WEBHOOK_URL', '')
    if not (send_webhook and url):
        return
    body = json.dumps({'content': message}).encode('utf-8')
    req = urllib.request.Request(
        url, data=body, method='POST',
        headers={'Content-Type': 'application/json',
                 'User-Agent': 'airflow-kelompok4/1.0'})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        log.error('Webhook gagal (alert tetap tercatat di file): %s', exc)


def on_success(context):
    _write_event(context, 'success')


def on_retry(context):
    _write_event(context, 'retry')


def on_failure(context):
    _write_event(context, 'failed')
    ti = context['task_instance']
    write_alert('Task GAGAL: ' + ti.dag_id + '.' + ti.task_id
                + ' (percobaan ke-' + str(ti.try_number) + ') | '
                + str(context.get('exception')))


def notify_run_failed(ti, run_id, **context):
    '''Task di DAG: hanya jalan bila ada task lain yang gagal (trigger_rule one_failed).'''
    write_alert('PIPELINE GAGAL: ' + ti.dag_id + ' | run ' + run_id
                + ' | detail di Airflow UI http://localhost:8080', send_webhook=True)


def cleanup_old_files(max_age_days=7, **context):
    '''Task di DAG: selalu jalan (trigger_rule all_done). Hapus file lama.'''
    cutoff = time.time() - max_age_days * 86400
    removed = 0
    for folder in (RAW_DIR, PROCESSED_DIR):
        for f in folder.glob('*'):
            if f.is_file() and f.name != '.gitkeep' and f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
    log.info('CLEANUP selesai | file dihapus=%d', removed)
