from airflow import DAG
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator # Mais leve que DummyOperator

with DAG(
    dag_id="00_MASTER_ORCHESTRATOR",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["orchestrator", "main"],
    # Limita quantas tarefas desta DAG podem rodar ao mesmo tempo para não travar o PC
    max_active_tasks=5, 
) as dag:

    # Pontos de sincronização (Gateways)
    start = EmptyOperator(task_id="start")
    join_ingestions = EmptyOperator(task_id="join_ingestions")
    join_sigef = EmptyOperator(task_id="join_sigef")
    join_satellites = EmptyOperator(task_id="join_satellites")
    end = EmptyOperator(task_id="end")

    # --- CAMADA 1: INGESTÕES ---
    ingestion_tasks = [
        TriggerDagRunOperator(task_id="t_ibama", trigger_dag_id="ingestion_ibama_to_bronze", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_car", trigger_dag_id="ingestion_car_to_bronze_optimized", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_mte", trigger_dag_id="ingestion_mte_slave_labor", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_reference", trigger_dag_id="ingestion_reference_brazil_to_bronze", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_hydro", trigger_dag_id="ingestion_brazil_hydro_to_bronze", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_mapbiomas", trigger_dag_id="ingestion_mapbiomas_intelligent", wait_for_completion=True),
    ]

    # --- CAMADA 2: SIGEF ---
    sigef_tasks = [
        TriggerDagRunOperator(task_id="t_sigef_am", trigger_dag_id="ingestion_sigef_to_bronze_am", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_sigef_mt", trigger_dag_id="ingestion_sigef_to_bronze_mt", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_sigef_pa", trigger_dag_id="ingestion_sigef_to_bronze_pa", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_sigef_ro", trigger_dag_id="ingestion_sigef_to_bronze_ro", wait_for_completion=True),
    ]

    # --- CAMADA 3: SATELLITE ---
    satellite_tasks = [
        TriggerDagRunOperator(task_id="t_sat_auditor", trigger_dag_id="satellite_app_auditor_pipeline", wait_for_completion=True),
        TriggerDagRunOperator(task_id="t_sat_ground", trigger_dag_id="satellite_ground_truth_pipeline", wait_for_completion=True),
    ]

    # --- CAMADA 4: DBT ---
    t_dbt = TriggerDagRunOperator(
        task_id="t_dbt_transformation",
        trigger_dag_id="dbt_transformation_medallion",
        wait_for_completion=True # Mudei para True para garantir que o 'end' só ocorra após o dbt
    )

    # --- FLUXO ORQUESTRADO ---
    start >> ingestion_tasks >> join_ingestions
    join_ingestions >> sigef_tasks >> join_sigef
    join_sigef >> satellite_tasks >> join_satellites
    join_satellites >> t_dbt >> end