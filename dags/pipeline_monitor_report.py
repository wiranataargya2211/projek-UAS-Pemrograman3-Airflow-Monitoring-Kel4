import csv
import logging
from collections import Counter, defaultdict

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from pipeline_utils.config import EVENTS_FILE, MONITORING_DIR, REPORT_FILE

log = logging.getLogger(__name__)
WINDOW = 50              # hanya hitung 50 event terakhir
MAX_FAILURE_RATE = 0.5   # ambang batas kegagalan: 50 persen


def _load_events():
    if not EVENTS_FILE.exists():
        return []
    with open(EVENTS_FILE, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))[-WINDOW:]


def build_report(**context):
    events = _load_events()
    if not events:
        log.warning('Belum ada event untuk dilaporkan')
        return 'kosong'
    per_task = defaultdict(Counter)
    for e in events:
        per_task[e['task_id']][e['state']] += 1
    MONITORING_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['task_id', 'success', 'retry', 'failed', 'failure_rate_pct'])
        for task_id, c in sorted(per_task.items()):
            done = c['success'] + c['failed']
            rate = round(100 * c['failed'] / done, 1) if done else 0
            writer.writerow([task_id, c['success'], c['retry'], c['failed'], rate])
            log.info('REPORT %s | sukses=%d retry=%d gagal=%d | gagal=%s persen',
                     task_id, c['success'], c['retry'], c['failed'], rate)
    log.info('Laporan tersimpan di %s', REPORT_FILE)
    return str(REPORT_FILE)


def health_check(**context):
    events = _load_events()
    done = [e for e in events if e['state'] in ('success', 'failed')]
    if not done:
        log.info('Belum ada data untuk dinilai')
        return
    failed = sum(1 for e in done if e['state'] == 'failed')
    rate = failed / len(done)
    log.info('HEALTH | gagal %d dari %d (%.0f persen)', failed, len(done), rate * 100)
    if rate > MAX_FAILURE_RATE:
        raise AirflowFailException(
            'Pipeline TIDAK SEHAT: kegagalan %.0f persen melebihi ambang %.0f persen'
            % (rate * 100, MAX_FAILURE_RATE * 100))


with DAG(
    dag_id='pipeline_monitor_report',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Jakarta'),
    schedule=None,                  # hanya jalan saat di-trigger DAG utama
    catchup=False,
    default_args={'owner': 'kelompok-4', 'retries': 0},
    tags=['kelompok-4', 'monitoring'],
) as dag:
    report = PythonOperator(task_id='build_report', python_callable=build_report)
    health = PythonOperator(task_id='health_check', python_callable=health_check)
    report >> health
