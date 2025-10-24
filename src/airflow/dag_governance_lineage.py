from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "gov-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="governance_lineage_daily",
    start_date=datetime(2025, 9, 24),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["governance","lineage"],
) as dag:

    inventory = BashOperator(
        task_id="inventory_flatten",
        bash_command="python /opt/airflow/dags/src/cli.py inventory --dir /opt/airflow/dags/data/s3_inventory --out /opt/airflow/dags/outputs/inventory"
    )

    lineage = BashOperator(
        task_id="build_lineage_graph",
        bash_command="python /opt/airflow/dags/src/cli.py lineage --edges /opt/airflow/dags/data/lineage/edges.csv --nodes /opt/airflow/dags/data/lineage/nodes.csv --out /opt/airflow/dags/artifacts/graph"
    )

    attach_dq = BashOperator(
        task_id="attach_dq_metrics",
        bash_command="python /opt/airflow/dags/src/cli.py attach --nodes /opt/airflow/dags/data/lineage/nodes.csv --dq /opt/airflow/dags/data/dq/table_quality.csv --out /opt/airflow/dags/artifacts/nodes_with_metrics.csv"
    )

    report = BashOperator(
        task_id="generate_report",
        bash_command="python /opt/airflow/dags/src/cli.py report --catalog /opt/airflow/dags/data/catalog --dq /opt/airflow/dags/data/dq/table_quality.csv --edges /opt/airflow/dags/data/lineage/edges.csv --out /opt/airflow/dags/reports"
    )

    inventory >> lineage >> attach_dq >> report
