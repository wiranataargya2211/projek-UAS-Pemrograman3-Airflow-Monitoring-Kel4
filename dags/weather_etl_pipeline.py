from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param

from pipeline_utils.callbacks import (cleanup_old_files, notify_run_failed,
                                      on_failure, on_retry, on_success)
from pipeline_utils.config import SCENARIOS
from pipeline_utils.etl_tasks import (extract_weather, load_weather,
                                      quality_check, transform_weather,
                                      validate_raw)

default_args = {
    'owner': 'kelompok-4',
    'retries': 2,                                  # ulang maksimal 2 kali
    'retry_delay': timedelta(seconds=20),          # jeda awal antar percobaan
    'retry_exponential_backoff': True,             # jeda makin lama tiap percobaan
    'max_retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=2),     # batas waktu tiap task
    'on_success_callback': on_success,
    'on_retry_callback': on_retry,
    'on_failure_callback': on_failure,
}

with DAG(
    dag_id='weather_etl_pipeline',
    description='ETL cuaca Surabaya dengan monitoring dan error handling',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Jakarta'),
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={'scenario': Param('normal', type='string', enum=SCENARIOS,
                              description='Pilih skenario untuk demo kegagalan')},
    tags=['kelompok-4', 'monitoring', 'error-handling'],
) as dag:

    start = EmptyOperator(task_id='start')

    extract = PythonOperator(
        task_id='extract_weather', python_callable=extract_weather,
        execution_timeout=timedelta(seconds=30))   # lebih ketat khusus extract
    validate = PythonOperator(task_id='validate_raw', python_callable=validate_raw)
    transform = PythonOperator(task_id='transform_weather', python_callable=transform_weather)
    quality = PythonOperator(task_id='quality_check', python_callable=quality_check)
    load = PythonOperator(task_id='load_weather', python_callable=load_weather)
    finish = EmptyOperator(task_id='finish')

    notify_failure = PythonOperator(
        task_id='notify_failure', python_callable=notify_run_failed,
        trigger_rule='one_failed')                 # hanya jalan jika ada yang gagal
    cleanup = PythonOperator(
        task_id='cleanup', python_callable=cleanup_old_files,
        trigger_rule='all_done')                   # selalu jalan, sukses ataupun gagal
    trigger_monitor = TriggerDagRunOperator(
        task_id='trigger_monitor', trigger_dag_id='pipeline_monitor_report',
        conf={'source_run_id': '{{ run_id }}'}, trigger_rule='all_done')

    start >> extract >> validate >> transform >> quality >> load >> finish

    core_tasks = [extract, validate, transform, quality, load]
    core_tasks >> notify_failure
    core_tasks >> cleanup >> trigger_monitor