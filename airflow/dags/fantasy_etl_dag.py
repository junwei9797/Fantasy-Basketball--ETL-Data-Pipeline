from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import pendulum

default_args = {
    'start_date': pendulum.datetime(2025, 6, 9, tz="Asia/Singapore"),
}
#Runs at 3pm daily
with DAG('fantasy_etl_dag', schedule='0 15 * * *', default_args=default_args, catchup=False) as dag:

    run_etl = DockerOperator(
        task_id='run_fantasy_etl',
        image='fantasy-etl-image:latest',
        container_name='fantasy_etl_container',
        command='python fantasy_etl.py',
        docker_url='unix://var/run/docker.sock',  # Default if running locally
        network_mode='fantasy_etl_network',
        auto_remove='success', # Remove container after run
        mounts=[ Mount(source='fantasy_etl_failed_data', target='/app/failed_records', type='volume')],  # You can define mounts if needed

    )

