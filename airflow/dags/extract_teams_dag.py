from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from docker.types import Mount


with DAG('extract_teams_dag',schedule=None,catchup=False) as dag:

    run_etl = DockerOperator(
        task_id='extract_teams',
        image='fantasy-etl-image:latest',
        container_name='extract_teams_etl_container',
        command='python pipeline/extract_team_pipeline/extract_team_pipeline.py',
        docker_url='unix://var/run/docker.sock',  # Default if running locally
        network_mode='fantasy_etl_network',
        auto_remove='success',  # Remove container after run
        mounts=[Mount(source='fantasy_etl_failed_data', target='/app/failed_records', type='volume')],

    )

    trigger_extract_players = TriggerDagRunOperator(
        task_id='trigger_extract_players_dag',
        trigger_dag_id='extract_players_dag',
        wait_for_completion=False  # This DAG would not wait for the other triggered DAG to complete
    )

    run_etl >> trigger_extract_players