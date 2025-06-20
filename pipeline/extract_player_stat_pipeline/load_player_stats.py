import logging
import pandas as pd
from datetime import date
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from pipeline import load_util

def load_player_career_stats (df: pd.DataFrame,table_name: str,engine,failedRows: list):
    logging.info(f"Processing data for '{table_name}'...")
    season_id = df['season_id'].iloc[0]
    player_id_tuple  = tuple(df['player_id'].tolist())
    try:
        with engine.begin() as conn:
            exists_query = text(f"select player_id,season_id from {table_name} where player_id in :player_id_list and season_id = :seasonId")
            result = pd.read_sql(exists_query,con=conn,params={'player_id_list': player_id_tuple, 'seasonId' : season_id})
            existing_keys = set(zip(result['player_id'], result['season_id']))
            df['key'] = list(zip(df['player_id'], df['season_id']))
            df_to_update = df[df['key'].isin(existing_keys)].copy()
            df_to_insert = df[~df['key'].isin(existing_keys)].copy()
            df_to_update.drop(columns='key', inplace=True)
            df_to_insert.drop(columns='key', inplace=True)
            df_to_insert = df_to_insert[['player_id','season_id','points','assists','rebounds','steals','blocks','turnovers','fantasy_points']]
            load_util.load_to_table(df_to_insert,table_name,engine,failedRows)

            for idx, row in df_to_update.iterrows():
                    logging.info(f"Player {row['player_id']} exists in current season: {season_id}, performing update")
                    update_query = text(f"""update {table_name} 
                    set points = points + :points,
                    assists = assists + :assists,
                    rebounds = rebounds + :rebounds,
                    steals = steals + :steals,
                    blocks = blocks + :blocks,
                    turnovers = turnovers + :turnovers,
                    fantasy_points = fantasy_points + :fantasy_points
                    where player_id = :playerId and season_id = :seasonId
                    """)
                    conn.execute(update_query, {'points': row['points'], 'assists': row['assists'], 'rebounds': row['rebounds'], 'steals': row['steals'], 'blocks': row['blocks'], 'turnovers': row['turnovers'],'fantasy_points': row['fantasy_points'], 'playerId': row['player_id'], 'seasonId' : season_id})
    except SQLAlchemyError as e:
        logging.error(f"Error inserting into {table_name}: {e}")
        failedRows.append({
            "table": table_name,
            "date": date.today(),
            "data": df.to_dict(orient='records'),
            "error": str(e)
        })