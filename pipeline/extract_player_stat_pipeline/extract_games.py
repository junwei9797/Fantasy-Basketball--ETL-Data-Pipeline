
import logging
import pandas as pd
from nba_api.live.nba.endpoints import scoreboard
from datetime import date

def extract_games():
    today = date.today()
    logging.info("Extracting nba games on date: %s", today)
    games_df = None
    try:
        board = scoreboard.ScoreBoard()
        if not board.get_dict().get('games', []):
            logging.info("No games found for today. Stopping extraction.")
            return games_df

        games = board.get_dict().get('games')
        #Extract necessary columns (game_id,game_date,home_team_id,away_team_id,season_id)
        games_df = pd.DataFrame(games)
    except Exception as e:
        logging.error(f"{e}")
    return games_df


extract_games()
