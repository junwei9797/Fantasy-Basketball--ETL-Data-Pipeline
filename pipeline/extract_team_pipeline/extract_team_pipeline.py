import logging
import pandas as pd
from pipeline.extract_team_pipeline import extract_teams
from pipeline import load_util

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_player_pipeline():
    logging.info('Starting extract_team_pipeline...')
    teams_df = extract_teams.extract_teams()
    if isinstance(teams_df, pd.DataFrame) and not teams_df.empty:
        engine = load_util.create_db_connection()
        load_util.load_to_table(teams_df,'team',engine,[])
    logging.info("extract_team_pipeline: done")

if __name__ == '__main__':
    extract_player_pipeline()