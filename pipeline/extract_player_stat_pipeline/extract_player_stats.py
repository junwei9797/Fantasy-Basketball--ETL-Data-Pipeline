import logging
import pandas as pd
from nba_api.stats.endpoints import BoxScoreTraditionalV2
from datetime import date
import pipeline.utils as utils

def extract_box_scores(df: pd.DataFrame):
    today = date.today()
    logging.info("Extracting box scores on date: %s", today)
    combined_box_scores_df = None
    box_scores = []
    try:
        for idx, row in df.iterrows():
            game_id = row['game_id']
            season_id = row['season_id']
            box_score = BoxScoreTraditionalV2(game_id=game_id)
            box_score_df = box_score.player_stats.get_data_frame()
            if box_score_df.empty:
                logging.info(f"No box score data for game_id {game_id}")
            box_score_df['season_id'] = season_id
            box_scores.append(box_score_df)
        combined_box_scores_df = pd.concat(box_scores, ignore_index=True)

    except Exception as e:
        logging.error(f"{e}")

    return combined_box_scores_df


def transform_player_career_stats(df: pd.DataFrame):
    today = date.today()
    logging.info("transforming player stats on date: %s", today)
    df = df[df['MIN'].notnull() & (df['MIN'] != '00:00')]
    df = df[
        ['PLAYER_ID', 'GAME_ID', 'season_id', 'PTS', 'AST', 'REB', 'STL', 'BLK', 'TO']].rename(columns={
        'PLAYER_ID': 'player_id',
        'GAME_ID': 'game_id',
        'PTS': 'points',
        'AST': 'assists',
        'REB': 'rebounds',
        'STL': 'steals',
        'BLK': 'blocks',
        'TO': 'turnovers'
    })
    numeric_cols = ["points", "rebounds", "assists", "steals", "blocks", "turnovers"]
    df[["points", "rebounds", "assists", "steals", "blocks", "turnovers"]] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df['fantasy_points'] = ((df["points"] * 1) + (df["rebounds"] * 1.2) + (df["assists"] * 1.5) + (df["steals"] * 3) + (df["blocks"] * 3) - (df["turnovers"] * 1)).round(1)
    df[['points','assists','rebounds','steals','blocks','turnovers']] = df[['points','assists','rebounds','steals','blocks','turnovers']].astype(int)
    return df
