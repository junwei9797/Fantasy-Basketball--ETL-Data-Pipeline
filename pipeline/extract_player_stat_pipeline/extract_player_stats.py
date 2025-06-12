import logging
import pandas as pd
from nba_api.live.nba.endpoints import boxscore
from datetime import date
import pipeline.utils as utils

def extract_player_stats(df: pd.DataFrame):
    today = date.today()
    logging.info("Extracting box scores on date: %s", today)
    extracted_player_stats = None
    try:
        for game in df['gameId']:
            box = boxscore.BoxScore(game)
            player_stats = pd.concat(
                [
                    pd.DataFrame(box.away_team_player_stats.get_dict()),
                    pd.DataFrame(box.home_team_player_stats.get_dict())
                ],
                ignore_index=True
            )
            player_stats = player_stats.loc[player_stats['played'] == '1', ["personId", "name", "statistics"]]
            extracted_player_stats  = pd.concat(player_stats)

    except Exception as e:
        logging.error(f"{e}")


def transform_player_career_stats(df: pd.DataFrame):
    today = date.today()
    logging.info("transforming player stats on date: %s", today)
    cols_to_keep = ['game_date','matchup','name','personId']
    cleaned_df = df[cols_to_keep].rename(columns={
        'game_date': 'game_date',
        'name': 'name',
        'matchup': 'matchup',
        'personId': 'player_id',
    })
    cleaned_df['season_id'] = utils.get_nba_season_id(cleaned_df['game_date'][0])
    statistics_df = pd.DataFrame(df['statistics'].tolist(),index=df.index)
    cleaned_df['points'] = statistics_df['points']
    cleaned_df['assists'] = statistics_df['assists']
    cleaned_df['rebounds'] = statistics_df['reboundsTotal']
    cleaned_df['steals'] = statistics_df['steals']
    cleaned_df['blocks'] = statistics_df['blocks']
    cleaned_df['turnovers'] = statistics_df['turnovers']
    cleaned_df['fantasy_points'] = ((cleaned_df["points"] * 1) + (cleaned_df["rebounds"] * 1.2) + (cleaned_df["assists"] * 1.5) + (cleaned_df["steals"] * 3) + (cleaned_df["blocks"] * 3) - (cleaned_df["turnovers"] * 1)).round(1)
    return cleaned_df

def transform_fantasy_score_per_game(df: pd.DataFrame):
    today = date.today()
    logging.info("transforming player stats on date: %s", today)
    cols_to_keep = ['game_date','matchup','name','personId']
    cleaned_df = df[cols_to_keep].rename(columns={
        'game_date': 'game_date',
        'name': 'name',
        'matchup': 'matchup',
        'personId': 'player_id',
    })
    cleaned_df['season_id'] = utils.get_nba_season_id(cleaned_df['game_date'][0])
    statistics_df = pd.DataFrame(df['statistics'].tolist(),index=df.index)
    cleaned_df['points'] = statistics_df['points']
    cleaned_df['assists'] = statistics_df['assists']
    cleaned_df['rebounds'] = statistics_df['reboundsTotal']
    cleaned_df['steals'] = statistics_df['steals']
    cleaned_df['blocks'] = statistics_df['blocks']
    cleaned_df['turnovers'] = statistics_df['turnovers']
    cleaned_df['fantasy_points'] = ((cleaned_df["points"] * 1) + (cleaned_df["rebounds"] * 1.2) + (cleaned_df["assists"] * 1.5) + (cleaned_df["steals"] * 3) + (cleaned_df["blocks"] * 3) - (cleaned_df["turnovers"] * 1)).round(1)
    return cleaned_df