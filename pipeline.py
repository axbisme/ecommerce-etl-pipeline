# pipeline.py

import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

from etl.transform import clean_data
from etl.load import load_data_to_db

# --------- Configure Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --------- Pipeline Settings ----------
RAW_FILE_NAME = "2019-Oct"
RAW_DATA_PATH = Path("data/raw/" + RAW_FILE_NAME + ".csv")
CLEAN_DATA_DIR = Path("data/clean")
CHUNKSIZE = 50_000
POSTGRES_URL = "postgresql://etl_user:etl_pass@localhost:5432/ecommerce_etl"
TABLE_NAME = "ecommerce_events"

def run_etl_pipeline():
    logger.info("Starting chunked ETL pipeline")

    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

    chunk_index = 0
    total_rows = 0
    for chunk in pd.read_csv(RAW_DATA_PATH, chunksize=CHUNKSIZE, nrows=3000000):
        logger.info(f"Processing chunk {chunk_index + 1}")
        try:
            clean_df = clean_data(chunk)
            total_rows += len(clean_df)

            output_path = CLEAN_DATA_DIR / f"clean_{RAW_FILE_NAME}_{chunk_index}.csv"
            clean_df.to_csv(output_path, index=False)
            logger.info(f"Saved cleaned chunk {chunk_index} with {len(clean_df)} rows")

            load_data_to_db(
                df=clean_df, 
                table_name=TABLE_NAME, 
                connection_url=POSTGRES_URL)

            chunk_index += 1
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_index}: {e}")

    logger.info("Finished processing %d chunks. Total cleaned rows: %d", chunk_index, total_rows)

if __name__ == "__main__":
    run_etl_pipeline()
