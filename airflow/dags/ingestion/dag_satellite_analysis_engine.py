import os
import time
import logging
import ee
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator

# --- CONFIGURAÇÕES GERAIS ---
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DEFAULT_ASSET_ID = "projects/agroesg-carbon-compliance/assets/agro_esg/targets_20260323"

default_args = {
    'owner': 'AgroESG',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'satellite_analysis_engine',
    default_args=default_args,
    description='Motor GEE: MapBiomas, SRTM e JAXA (Separados)',
    schedule_interval="0 2 * * 1", # Segunda-feira às 02:00
    catchup=False,
    tags=['satellite', 'gee'],
) as dag:

    @task
    def trigger_gee_analyses(ds_nodash: str, **kwargs):
        # Importamos as 3 funções separadas do handler
        from utils.gee_handler import (
            export_topography_stats, 
            export_elevation_stats, 
            export_mapbiomas_stats
        )
        
        asset_id = kwargs['dag_run'].conf.get('asset_id', DEFAULT_ASSET_ID)
        output_prefix = f"satellite/results/run_{ds_nodash}"
        
        task_ids = []
        
        # 1. Topografia (SRTM) - Separado
        task_ids.append(export_topography_stats(asset_id, BUCKET_NAME, output_prefix))
        
        # 2. Elevação (JAXA) - Separado
        task_ids.append(export_elevation_stats(asset_id, BUCKET_NAME, output_prefix))
        
        # 3. Uso do Solo (MapBiomas) - Separado
        task_ids.append(export_mapbiomas_stats(asset_id, 2022, BUCKET_NAME, output_prefix))
        
        logging.info(f"✅ 3 tarefas enviadas ao GEE: {task_ids}")
        return task_ids

    @task
    def wait_for_gee_exports(task_ids: list):
        from utils.gee_handler import initialize_gee
        initialize_gee()
        
        flat_task_ids = []
        for item in task_ids:
            if isinstance(item, list): flat_task_ids.extend(item)
            else: flat_task_ids.append(item)
        
        pending_tasks = set(filter(None, flat_task_ids))
        
        while pending_tasks:
            for tid in list(pending_tasks):
                status = ee.data.getTaskStatus(tid)[0]
                state = status['state']
                
                if state == 'COMPLETED':
                    logging.info(f"✅ Concluída: {tid}")
                    pending_tasks.remove(tid)
                elif state in ['FAILED', 'CANCELLED']:
                    raise Exception(f"❌ Falha no GEE ({tid}): {status.get('error_message')}")
            
            if pending_tasks:
                time.sleep(60)
                
        return "CSVs prontos no GCS."

    load_results_to_bq = GCSToBigQueryOperator(
        task_id='load_results_to_bq',
        bucket=BUCKET_NAME,
        source_objects=[f"satellite/results/run_{{{{ ds_nodash }}}}_*.csv"],
        destination_project_dataset_table="agro_esg_raw.raw_satellite_results",
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=1,
        autodetect=True,
        gcp_conn_id='google_cloud_default'
    )

    run_dbt_fct = BashOperator(
        task_id='run_dbt_fct',
        bash_command='cd /home/obscuritenoir/Portfolio/AgroESG-Carbon-Compliance/agro_credit_transform && dbt build --select fct_compliance_final',
    )

    # Fluxo
    gee_ids = trigger_gee_analyses(ds_nodash="{{ ds_nodash }}")
    wait_sensor = wait_for_gee_exports(gee_ids)
    
    wait_sensor >> load_results_to_bq >> run_dbt_fct