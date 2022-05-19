from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils import timezone

args = {
    'owner': 'Airflow',
}
with DAG(
        dag_id='example_spark_operator',
        default_args=args,
        schedule_interval=None,
        start_date=days_ago(2),
        tags=['example'],
) as dag:

    python_submit_job = SparkSubmitOperator(
        task_id='python_job',
        conn_id='spark-local',
        application="/Users/fatmik/PycharmProjects/NYC_Taxi_Project/DataPreparing.py",
        dag=dag
    )

    python_submit_job

    # [END howto_operator_spark_submit]

