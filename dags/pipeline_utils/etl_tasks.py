import csv
import json
import logging
import shutil
import urllib.request
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowFailException

from pipeline_utils.config import API_URL, OUTPUT_DIR, PROCESSED_DIR, RAW_DIR
from pipeline_utils.failure_sim import corrupt_payload, maybe_fail

log = logging.getLogger(__name__)
REQUIRED_KEYS = ['time', 'temperature_2m', 'relative_humidity_2m', 'precipitation']


def _now():
    return datetime.now().strftime('%Y%m%d_%H%M%S')


def _scenario(params):
    return params.get('scenario', 'normal')


def extract_weather(params, **context):
    scenario = _scenario(params)
    log.info('EXTRACT mulai | skenario=%s', scenario)
    maybe_fail(scenario, 'extract')
    try:
        with urllib.request.urlopen(API_URL, timeout=20) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
    except Exception as exc:
        log.error('EXTRACT gagal memanggil API: %s', exc)
        raise
    if scenario == 'bad_data':
        payload = corrupt_payload(payload)
        log.warning('EXTRACT: data sengaja dirusak (skenario bad_data)')
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / ('weather_raw_' + _now() + '.json')
    path.write_text(json.dumps(payload), encoding='utf-8')
    log.info('EXTRACT selesai | file=%s', path)
    return str(path)  # otomatis disimpan sebagai XCom untuk task berikutnya


def validate_raw(params, ti, **context):
    maybe_fail(_scenario(params), 'validate')
    path = ti.xcom_pull(task_ids='extract_weather')
    payload = json.loads(Path(path).read_text(encoding='utf-8'))
    hourly = payload.get('hourly')
    if not hourly:
        raise AirflowFailException('Struktur JSON tidak memiliki bagian hourly')
    missing = [k for k in REQUIRED_KEYS if k not in hourly]
    if missing:
        raise AirflowFailException('Kolom wajib hilang: ' + ', '.join(missing))
    if len(hourly['time']) == 0:
        raise AirflowFailException('Data kosong (0 baris)')
    log.info('VALIDATE lolos | jumlah baris=%d', len(hourly['time']))
    return path


def categorize(temp):
    if temp is None:
        return 'unknown'
    if temp >= 33:
        return 'panas'
    if temp >= 28:
        return 'hangat'
    return 'sejuk'


def transform_weather(params, ti, **context):
    maybe_fail(_scenario(params), 'transform')
    path = ti.xcom_pull(task_ids='validate_raw')
    hourly = json.loads(Path(path).read_text(encoding='utf-8'))['hourly']
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / ('weather_clean_' + _now() + '.csv')
    rows = zip(hourly['time'], hourly['temperature_2m'],
               hourly['relative_humidity_2m'], hourly['precipitation'])
    count = 0
    with open(out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['time', 'temp_c', 'humidity_pct', 'rain_mm', 'heat_category'])
        for t, temp, hum, rain in rows:
            writer.writerow([t, temp, hum, rain, categorize(temp)])
            count += 1
    log.info('TRANSFORM selesai | baris=%d | file=%s', count, out)
    return str(out)


def quality_check(params, ti, **context):
    maybe_fail(_scenario(params), 'quality')
    path = ti.xcom_pull(task_ids='transform_weather')
    with open(path, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    problems = []
    if len(rows) < 24:
        problems.append('jumlah baris kurang dari 24 (ada ' + str(len(rows)) + ')')
    for i, r in enumerate(rows, start=2):
        if r['temp_c'] == '':
            problems.append('baris ' + str(i) + ': suhu kosong')
            continue
        if not 15 <= float(r['temp_c']) <= 45:
            problems.append('baris ' + str(i) + ': suhu di luar 15-45 C (' + r['temp_c'] + ')')
        if not 0 <= float(r['humidity_pct']) <= 100:
            problems.append('baris ' + str(i) + ': kelembapan di luar 0-100')
    if problems:
        for p in problems:
            log.error('QUALITY: %s', p)
        raise AirflowFailException(
            'Quality check gagal: ' + str(len(problems)) + ' masalah. Contoh: ' + problems[0])
    log.info('QUALITY lolos | baris=%d', len(rows))
    return path


def load_weather(params, ti, **context):
    maybe_fail(_scenario(params), 'load')
    path = ti.xcom_pull(task_ids='quality_check')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    target = OUTPUT_DIR / 'weather_latest.csv'
    shutil.copyfile(path, target)
    log.info('LOAD selesai | hasil akhir=%s', target)
    return str(target)
