from airflow import DAG
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="00_MASTER_ORCHESTRATOR",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["orchestrator", "main"],
) as dag:

    # --- CAMADA 1: INGESTÕES DE BASE E COMPLIANCE ---
    t_ibama = TriggerDagRunOperator(task_id="t_ibama", trigger_dag_id="ingestion_ibama_to_bronze", wait_for_completion=True)
    t_car = TriggerDagRunOperator(task_id="t_car", trigger_dag_id="ingestion_car_to_bronze_optimized", wait_for_completion=True)
    t_mte = TriggerDagRunOperator(task_id="t_mte", trigger_dag_id="ingestion_mte_slave_labor", wait_for_completion=True)
    t_ref = TriggerDagRunOperator(task_id="t_reference", trigger_dag_id="ingestion_reference_brazil_to_bronze", wait_for_completion=True)
    t_hydro = TriggerDagRunOperator(task_id="t_hydro", trigger_dag_id="ingestion_brazil_hydro_to_bronze", wait_for_completion=True)
    t_mapbiomas = TriggerDagRunOperator(task_id="t_mapbiomas", trigger_dag_id="ingestion_mapbiomas_intelligent", wait_for_completion=True)

    # --- CAMADA 2: SIGEF ---
    t_sigef_am = TriggerDagRunOperator(task_id="t_sigef_am", trigger_dag_id="ingestion_sigef_to_bronze_am", wait_for_completion=True)
    t_sigef_mt = TriggerDagRunOperator(task_id="t_sigef_mt", trigger_dag_id="ingestion_sigef_to_bronze_mt", wait_for_completion=True)
    t_sigef_pa = TriggerDagRunOperator(task_id="t_sigef_pa", trigger_dag_id="ingestion_sigef_to_bronze_pa", wait_for_completion=True)
    t_sigef_ro = TriggerDagRunOperator(task_id="t_sigef_ro", trigger_dag_id="ingestion_sigef_to_bronze_ro", wait_for_completion=True)

    # --- CAMADA 3: SATELLITE ---
    t_sat_auditor = TriggerDagRunOperator(task_id="t_sat_auditor", trigger_dag_id="satellite_app_auditor_pipeline", wait_for_completion=True)
    t_sat_ground = TriggerDagRunOperator(task_id="t_sat_ground", trigger_dag_id="satellite_ground_truth_pipeline", wait_for_completion=True)

    # --- CAMADA 4: DBT ---
    t_dbt = TriggerDagRunOperator(
        task_id="t_dbt_transformation",
        trigger_dag_id="dbt_transformation_medallion",
        wait_for_completion=False 
    )

    # --- FLUXO CORRIGIDO (Sem erro de lista >> lista) ---
    
    # 1. Agrupamentos
    step_1_bases = [t_ibama, t_car, t_mte, t_ref, t_hydro, t_mapbiomas]
    step_2_sigef = [t_sigef_am, t_sigef_mt, t_sigef_pa, t_sigef_ro]
    step_3_satellites = [t_sat_auditor, t_sat_ground]

    # 2. Encadeamento correto:
    # Cada elemento da lista 1 aponta para a lista 2, e assim por diante
    for base_task in step_1_bases:
        base_task >> step_2_sigef

    for sigef_task in step_2_sigef:
        sigef_task >> step_3_satellites

    for satellite_task in step_3_satellites:
        satellite_task >> t_dbt
