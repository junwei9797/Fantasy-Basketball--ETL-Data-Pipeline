import argparse
import logging
import pandas as pd
from pipeline.extract_player_stat_pipeline import extract_games,extract_player_stats,load_player_stats
from pipeline import load_util

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#Main method to execute
def extract_player_stat_pipeline(extract_date= None):
    logging.info('Starting extract_player_stat_pipeline...')
    games_df = extract_games.extract_games(extract_date)
    if isinstance(games_df, pd.DataFrame) and not games_df.empty:
       box_scores_df = extract_player_stats.extract_box_scores(games_df,extract_date)
       if isinstance(box_scores_df, pd.DataFrame) and not box_scores_df.empty:
           box_scores_df = extract_player_stats.transform_player_career_stats(box_scores_df)
           engine = load_util.create_db_connection()
           load_util.load_to_table(games_df, 'game', engine, [])
           load_util.load_to_table(box_scores_df, 'fantasy_score_per_game', engine, [])
           load_player_stats.load_player_career_stats(box_scores_df,'player_career_stats', engine, [])
    logging.info("extract_player_stat_pipeline: done")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--extract_date', type=str, required=False, help='Date to extract (YYYY-MM-DD)')
    args = parser.parse_args()
    extract_player_stat_pipeline(extract_date=args.extract_date)