import logging
from datetime import date
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine,MetaData,inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert


def load_to_table(df: pd.DataFrame,table_name: str,engine,failedRows: list):
        try:
            logging.info(f"Loading data into table '{table_name}'...")
            df.to_sql(table_name, engine, if_exists='append', index=False, schema='public')
            logging.info(f"Successfully loaded {len(df)} records into '{table_name}'.")
        except SQLAlchemyError as e:
            logging.error(f"Error loading data into '{table_name}': {e}")
        #     failedRows.append({
        #         "table": table_name,
        #         "date": date.today(),
        #         "data": df.to_dict(orient='records'),
        #         "error": str(e)
        #     })
        # if failedRows:
        #     create_failed_records_csv(table_name,failedRows)


def upsert_to_table(df: pd.DataFrame,table_name: str,engine,failedRows: list):
    try:
        logging.info(f"Loading data into table '{table_name}'...")
        metadata = MetaData()
        metadata.reflect(bind=engine)
        table = metadata.tables.get(table_name)
        primary_keys = get_primary_keys(engine, table_name)
        if not primary_keys:
            raise Exception(f"No primary key found for table {table_name}")
        records = df.to_dict(orient="records")
        stmt = pg_insert(table).values(records)
        update_cols = {col.name: stmt.excluded[col.name]
                       for col in table.columns
                       if col.name not in primary_keys}

        upsert_stmt = stmt.on_conflict_do_update(index_elements=primary_keys,set_=update_cols)
        with engine.begin() as conn:
            result = conn.execute(upsert_stmt)
            logging.info(f"[{table_name}] updated/inserted successful: {result.rowcount} rows affected.")

    except Exception as e:
        logging.error(f"Upsert failed for table {table_name}: {e}")
    #     failedRows.append({
    #         "table": table_name,
    #         "date": date.today(),
    #         "data": df.to_dict(orient='records'),
    #         "error": str(e)
    #     })
    # if failedRows:
    #     create_failed_records_csv(table_name, failedRows)



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

def get_primary_keys(engine, table_name, schema='public'):
    inspector = inspect(engine)
    primary_keys = inspector.get_pk_constraint(table_name)['constrained_columns']
    return primary_keys


def create_failed_records_csv(table_name:str,failed_records: list):
    flat_df = pd.json_normalize(
        failed_records,
        record_path='data',
        meta=['table', 'date','data','error']
    )
    #Use append instead of overwrite
    flat_df.to_csv(f'/app/failed_records/{table_name}_failed_records_{date.today()}.csv',mode='a', index=False)