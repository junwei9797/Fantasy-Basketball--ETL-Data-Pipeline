import logging
from datetime import date
import pandas as pd
from nba_api.stats.static import teams

def extract_teams():
    today = date.today()
    logging.info("Extracting nba teams on date: %s", today)
    teams_df = None
    try:
        all_teams = teams.get_teams()
        if not all_teams:
            logging.info("No teams found. Stop extraction")
            return None
        teams_df = pd.DataFrame(all_teams,columns=['id', 'full_name', 'abbreviation'])
        teams_df['abbreviation'] = teams_df['abbreviation'].astype(str).str.slice(0, 3)
        teams_df.rename(columns={'id':'team_id','full_name':'team_name'}, inplace=True)
        logging.info(f"Extracted {len(teams_df)} teams.")

    except Exception as e:
        logging.error(f"Failed extraction for nba teams: {e}")

    return teams_df


