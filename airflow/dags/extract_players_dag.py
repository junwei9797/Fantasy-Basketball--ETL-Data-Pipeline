from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


with DAG('extract_players_dag',schedule=None,catchup=False) as dag:


    run_etl = DockerOperator(
        task_id='extract_players',
        image='fantasy-etl-image:latest',
        container_name='extract_players_etl_container',
        command='python pipeline/extract_player_pipeline/extract_player_pipeline.py',
        docker_url='unix://var/run/docker.sock',  # Default if running locally
        network_mode='fantasy_etl_network',
        auto_remove='success',  # Remove container after run
        mounts=[Mount(source='fantasy_etl_failed_data', target='/app/failed_records', type='volume')],

    )