import logging
import pandas as pd
from datetime import date
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


def load_player_career_stats (df: pd.DataFrame,table_name: str,engine,failedRows: list):
    logging.info(f"Loading data into table '{table_name}'...")
    season_id = df['season_id'].iloc[0]
    player_id_list = df['player_id'].tolist()
    with engine.begin() as conn:
        exists_query = text(f"select * from {table_name} where player_id in :player_id_list and season_id = :seasonId")
        exist_df = pd.read_sql('exists_query',{'player_id_list': player_id_list, 'seasonId' : season_id})

    try:
        with engine.begin() as conn:
            for idx, row in df.iterrows():
                exists_query = text(f"select 1 from {table_name} where player_id = :playerId and season_id = :seasonId")
                exists = conn.execute(exists_query, {'playerId': row['player_id'], 'seasonId' : season_id}).fetchone()
                if exists:
                    logging.info(f"Player {row['name']} exists in current season: {season_id}, performing update")
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
                else:
                    logging.info(f"Player {row['name']} dont exist in current season: {season_id}, performing insert")
                    row[['player_id', 'season_id', 'name', 'points', 'assists','rebounds', 'steals', 'blocks', 'turnovers', 'fantasy_points']].to_frame().T.to_sql(table_name,engine,if_exists='append',index=False,schema='public')
    except SQLAlchemyError as e:
        logging.error(f"Error inserting into {table_name}: {e}")
        failedRows.append({
            "table": table_name,
            "date": date.today(),
            "data": df.to_dict(orient='records'),
            "error": str(e)
        })
    df_check = pd.read_sql(f"SELECT COUNT(1) FROM {table_name};", engine)
    logging.debug(df_check)