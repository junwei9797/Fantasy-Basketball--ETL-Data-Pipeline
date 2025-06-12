import logging
import time
from datetime import date

import numpy as np
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo


def extract_players():
    today = date.today()
    players_df = None
    player_data = []
    try:
        logging.info("Extracting nba players on date: %s", today)
        all_players = players.get_active_players()
        if not all_players:
            logging.info("No players found. Stop extraction")
            return None

        for player in all_players:
            logging.info(f"extracting player {player['id']}")
            info = commonplayerinfo.CommonPlayerInfo(player['id']).get_data_frames()[0][['PERSON_ID','DISPLAY_FIRST_LAST','TEAM_ID','POSITION', 'HEIGHT', 'WEIGHT', 'BIRTHDATE']]
            player_data.append(info.iloc[0].to_dict())
            time.sleep(1)
        players_df = pd.DataFrame(player_data)
        logging.info(f"Extracted {len(players_df)} players.")

    except Exception as e:
        logging.error(f"Failed extraction for nba players: {e}")
    return players_df

def transform_players(df: pd.DataFrame):
    today = date.today()
    logging.info("Transforming nba players data on date: %s", today)
    cur_df_size = len(df)
    df = df.rename(
                columns={
                        'PERSON_ID': 'player_id',
                        'DISPLAY_FIRST_LAST': 'name',
                        'TEAM_ID': 'team_id',
                        'POSITION': 'position',
                        'HEIGHT': 'height',
                        'WEIGHT': 'weight',
                        'BIRTHDATE': 'birth_date'
                        })
    cols = ['player_id', 'name', 'team_id', 'position', 'height', 'weight', 'birth_date']
    df[cols] = df[cols].replace(r'^\s*$', np.nan, regex=True)
    df = df.dropna(subset=['player_id', 'name'])
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce').dt.date
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce').round(1)
    logging.info(f"Players count before transformation: {cur_df_size}, after transformation: {len(df)}")
    return df