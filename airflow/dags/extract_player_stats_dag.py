from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import pendulum

default_args = {
    'start_date': pendulum.datetime(2025, 6, 9, tz="Asia/Singapore"),
}
#Runs at 3pm daily
with DAG('extract_player_stats_dag', schedule='0 15 * * *', default_args=default_args, catchup=False) as dag:

    run_etl = DockerOperator(
        task_id='extract_player_stats',
        image='fantasy-etl-image:latest',
        container_name='extract_player_stats_etl_container',
        command='python pipeline/extract_player_stat_pipeline/extract_player_stat_pipeline.py',
        docker_url='unix://var/run/docker.sock',  # Default if running locally
        network_mode='fantasy_etl_network',
        auto_remove='success', # Remove container after run
        mounts=[ Mount(source='fantasy_etl_failed_data', target='/app/failed_records', type='volume')],  # You can define mounts if needed

    )
