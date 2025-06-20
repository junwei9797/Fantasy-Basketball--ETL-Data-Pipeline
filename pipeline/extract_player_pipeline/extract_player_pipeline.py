import logging
import pandas as pd
from pipeline.extract_player_pipeline import extract_players
from pipeline import load_util

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_player_pipeline():
    logging.info('Starting extract_player_pipeline...')
    players_df = extract_players.extract_players()
    if isinstance(players_df, pd.DataFrame) and not players_df.empty:
        players_df = extract_players.transform_players(players_df)
        engine = load_util.create_db_connection()
        load_util.upsert_to_table(players_df,'player',engine,[])
    logging.info("extract_player_pipeline: done")

if __name__ == '__main__':
    extract_player_pipeline()