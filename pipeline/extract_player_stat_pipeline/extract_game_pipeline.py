
import logging
import pandas as pd
from pipeline.extract_player_stat_pipeline import extract_games
from pipeline import load_util
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#Main method to execute
def extract_player_stat_pipeline():
    logging.info('Starting extract_player_stat_pipeline...')
    games_df = extract_games.extract_games()
    if isinstance(games_df, pd.DataFrame) and not games_df.empty:
       extracted_player_stats = extract_games.extract_player_stats(games_df)
       engine = load_util.create_db_connection()
       load_util.load_to_table(games_df, 'game', engine, [])
       load_util.load_to_table(games_df, 'fantasy_score_per_game', engine, [])
       load_util.load_to_table(games_df, 'player_career_stats', engine, [])
       logging.info("extract_player_stat_pipeline: done")