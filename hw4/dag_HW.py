import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator

from datetime import timedelta


dag_spark = DAG(
        dag_id = "sparkoperator_try",
        default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30)
        },
        schedule_interval="@once",
        dagrun_timeout=timedelta(minutes=5),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)


transfer_clean_data_py = SFTPOperator(
    task_id="transfer_clean_data_py",
    ssh_conn_id="sftp-cluster",
    local_filepath="/home/artem/files/clean_data.py",
    remote_filepath="/home/ubuntu/clean_data.py",
    operation="put",
    create_intermediate_dirs=True,
    dag=dag_spark
)

run_clean_data_py = SSHOperator(
    ssh_conn_id="ssh-cluster",
    task_id="pip-install",
    command="pip install findspark boto3",
)

run_clean_data_py = SSHOperator(
    ssh_conn_id="ssh-cluster",
    task_id="ssh-run",
    command="python /home/ubuntu/clean_data.py",
)

transfer_clean_data_py >> run_clean_data_py

if __name__ == '__main__ ':
    dag_spark.cli()