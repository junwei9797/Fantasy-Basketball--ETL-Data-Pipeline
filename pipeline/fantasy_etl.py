"""
File used to extract nba scores from Basketball Reference website
"""
import logging
import time
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
from nba_api.live.nba.endpoints import scoreboard,boxscore
from datetime import datetime, date

#Main method to execute
def fantasy_etl_pipeline():
    all_game_logs = []
    extracted_values = extract()
    for player_stats in extracted_values:
            cleaned_df = game_logs_transform(player_stats)
            all_game_logs.append(cleaned_df)

    combined_game_logs_df = pd.concat(all_game_logs, ignore_index=True)
    load(combined_game_logs_df,{'fantasy_score_per_game': 'fantasy_score_per_game','player_career_stats' : 'player_career_stats'})
    return combined_game_logs_df

#Method to extract player statistics from NBA API, return list of extracted player statistics
def extract():
    logging.info("Extracting nba scores on date: ", date.today()
)
    extracted_player_stats = []

    try:
        board = scoreboard.ScoreBoard()
        games = board.games.get_dict()
        for game in games:
            box = boxscore.BoxScore(game['gameId'])
            player_stats = pd.concat(
                [
                    pd.DataFrame(box.away_team_player_stats.get_dict()),
                    pd.DataFrame(box.home_team_player_stats.get_dict())
                ],
                ignore_index=True
            )
            player_stats = player_stats.loc[player_stats['played'] == '1', ["personId", "name", "statistics"]]
            player_stats["matchup"] = game['awayTeam']['teamName'] + " vs. " + game['homeTeam']['teamName']
            player_stats["game_date"] = datetime.strptime(board.score_board_date.get('game_date'), "%Y-%m-%d").date()
            extracted_player_stats.append(player_stats)
            time.sleep(0.6)

    except Exception as e:
        logging.error(f"{e}")

    return extracted_player_stats

#Method to perform data transformation on player statistics, return individual player stats dataframe
def game_logs_transform(df: pd.DataFrame):
    logging.info("Performing player stats data transformation on date: ", date.today())
    cols_to_keep = ['game_date','matchup','name','personId']
    cleaned_df = df[cols_to_keep].rename(columns={
        'game_date': 'game_date',
        'name': 'name',
        'matchup': 'matchup',
        'personId': 'player_id',
    })
    cleaned_df['season_id'] = get_nba_season_id(cleaned_df['game_date'][0])
    statistics_df = pd.DataFrame(df['statistics'].tolist(),index=df.index)
    cleaned_df['points'] = statistics_df['points']
    cleaned_df['assists'] = statistics_df['assists']
    cleaned_df['rebounds'] = statistics_df['reboundsTotal']
    cleaned_df['steals'] = statistics_df['steals']
    cleaned_df['blocks'] = statistics_df['blocks']
    cleaned_df['turnovers'] = statistics_df['turnovers']
    cleaned_df['fantasy_points'] = ((cleaned_df["points"] * 1) + (cleaned_df["rebounds"] * 1.2) + (cleaned_df["assists"] * 1.5) + (cleaned_df["steals"] * 3) + (cleaned_df["blocks"] * 3) - (cleaned_df["turnovers"] * 1)).round(1)
    return cleaned_df

#Method to load transformed player statistics to respective db tables
def load(df: pd.DataFrame,table_names: dict):
    logging.info("Loading transformed data to database tables on date: ", date.today())
    failed_rows = []
    try:
        engine = create_db_connection()
        game_logs_load(df,table_names.get('fantasy_score_per_game'),engine,failed_rows)
        career_stats_load(df, table_names.get('player_career_stats'), engine,failed_rows)
    except SQLAlchemyError as e:
        logging.debug(e)

    if failed_rows:
        create_failed_records_csv(failed_rows)


def game_logs_load(df: pd.DataFrame,table_name: str,engine,failedRows: list):

    try:
        df.to_sql('table_name', engine, if_exists='append', index=False, schema='public')
        logging.info("fantasy_score_per_game records inserted successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error inserting into {table_name}: {e}")
        failedRows.append({
            "table": table_name,
            "date" : date.today(),
            "data": df.to_dict(orient='records'),
            "error": str(e)
        })
    df_check = pd.read_sql(f"SELECT COUNT(1) FROM {table_name};", engine)
    logging.debug(df_check)

def career_stats_load(df: pd.DataFrame,table_name: str,engine,failedRows: list):
    season_id = df['season_id'].iloc[0]
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
                    logging.debug(f"Player {row['name']} dont exist in current season: {season_id}, performing insert")
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

def get_nba_season_id(date):
    """
    Given a date (datetime.date or datetime.datetime), return the NBA season string ID, e.g., '2023-24'
    """
    year = date.year
    if date.month >= 10:  # NBA season starts in October
        start_year = year
        end_year = year + 1
    else:
        start_year = year - 1
        end_year = year

    return f"{start_year}-{str(end_year)[-2:]}"


def create_db_connection():
    load_dotenv()
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "fantasy_basketball_db")
    connection_uri = f'postgresql+psycopg2://{user}:{password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_uri)
    return engine

def create_failed_records_csv(failed_records: list):
    flat_df = pd.json_normalize(
        failed_records,
        record_path='data',
        meta=['table', 'date','data','error']
    )
    flat_df.to_csv(f'failed_rows{date.today()}.csv', index=False)