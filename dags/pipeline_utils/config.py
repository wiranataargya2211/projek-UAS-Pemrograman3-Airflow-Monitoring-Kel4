from pathlib import Path

# Folder di DALAM container (di-mount dari folder ./data di laptop)
DATA_DIR = Path('/opt/airflow/data')
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'
OUTPUT_DIR = DATA_DIR / 'output'
MONITORING_DIR = DATA_DIR / 'monitoring'

EVENTS_FILE = MONITORING_DIR / 'task_events.csv'
ALERT_LOG = MONITORING_DIR / 'alerts.log'
REPORT_FILE = MONITORING_DIR / 'report_latest.csv'

# Prakiraan cuaca per jam untuk Surabaya (Open-Meteo, gratis tanpa API key)
API_URL = (
    'https://api.open-meteo.com/v1/forecast'
    '?latitude=-7.25&longitude=112.75'
    '&hourly=temperature_2m,relative_humidity_2m,precipitation'
    '&timezone=Asia%2FJakarta&forecast_days=2'
)

# Skenario yang bisa dipilih saat men-trigger DAG secara manual
SCENARIOS = ['normal', 'api_error', 'bad_data', 'slow_api', 'random_fail']
