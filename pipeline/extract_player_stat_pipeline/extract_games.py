
import logging
import pandas as pd
from datetime import date, datetime
from nba_api.stats.endpoints import ScoreboardV2
from pipeline import utils

def extract_games(extract_date= None):
    if extract_date is None:
        extract_date = date.today()
        formatted_date = utils.get_nba_est_date()
    else:
        formatted_date = datetime.strptime(extract_date, "%Y-%m-%d").date()
    logging.info("Extracting nba games for date: %s", extract_date)
    games_df = None
    try:
        scoreboard = ScoreboardV2(league_id = "00",game_date=formatted_date)
        games_df = scoreboard.game_header.get_data_frame()[['GAME_ID','GAME_DATE_EST','HOME_TEAM_ID','VISITOR_TEAM_ID']].rename(columns={
            'GAME_ID' : 'game_id',
            'GAME_DATE_EST':'game_date',
            'HOME_TEAM_ID':'home_team_id',
            'VISITOR_TEAM_ID':'away_team_id',
        })
        if games_df.empty:
            logging.info("No games found. Stop extraction")
            return None
        games_df['game_date'] = pd.to_datetime(games_df['game_date'], errors='coerce').dt.date
        games_df['season_id'] = games_df['game_date'].apply(utils.get_nba_season_id)
    except Exception as e:
        logging.error(f"{e}")
    return games_df
