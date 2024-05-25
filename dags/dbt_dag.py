import os
from datetime import datetime
from airflow import DAG

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig,DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from fivetran_provider_async.operators import FivetranOperator


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "retail_analytics", "schema": "raw"},
    )
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dbt_snowflake_dag = DbtDag(
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"),
        dag_id = "dbt_dagg"
    )

with DAG(
    dag_id="dbt_dag_2",
    default_args=default_args,
    description="Second DBT and Fivetran DAG",
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
) as dag2:
    
        
    fivetran_sync_start_1 = FivetranOperator(
    task_id="fivetran_mysql_snowflake_1",
    fivetran_conn_id="fivetran_conn",
    connector_id="{{ var.value.fivetran_conn_id }}",
    deferrable=False,
    )



    fivetran_sync_start_1 >> dbt_task