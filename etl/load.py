import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def load_data_to_db(df: pd.DataFrame, table_name: str, connection_url: str):
    """
    Loads cleaned DataFrame into a PostgreSQL table.
    """
    try:
        engine = create_engine(connection_url)

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # replace for test tables; use 'append' for prod
            index=False,
            chunksize=1000
        )

        logger.info(f"Loaded {len(df)} rows into table '{table_name}'")
    except Exception as e:
        logger.error(f"Failed to load data to Postgres: {e}")
        raise
