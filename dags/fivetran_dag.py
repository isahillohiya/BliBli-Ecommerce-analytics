from datetime import datetime
import os
from airflow import DAG
from fivetran_provider_async.operators import FivetranOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig,DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


# Default arguments for all DAGs
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "retail_analytics", "schema": "raw"},
    )
)

# First DAG
with DAG(
    dag_id="fivetran_dag",
    default_args=default_args,
    description="Fivetran DAG",
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
) as dag1:

    fivetran_sync_start_1 = FivetranOperator(
        task_id="fivetran_mysql_snowflake_1",
        fivetran_conn_id="fivetran_conn",
        connector_id="{{ var.value.fivetran_conn_id }}",
        deferrable=False,
    )

    dbt_task = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/blibli/"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"),
        operator_args={"install_deps": True},
        default_args={"retries": 2},
    )

    fivetran_sync_start_1>>dbt_task