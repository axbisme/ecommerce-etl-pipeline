# transform.py

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def init_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s"
    )


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms raw e-commerce data.
    - Converts data types
    - Fills missing values
    - Drops invalid rows and duplicates
    - Validates cleaned dataset
    """
    logger.info("Begin data cleaning. Rows available: %d", len(df))

    logger.info("Converting data types...")
    df['event_time'] = pd.to_datetime(df['event_time'])
    df['price'] = pd.to_numeric(df['price'], errors='coerce').astype(float)

    columns_to_convert_str = ['product_id', 'user_id', 'category_id']
    df[columns_to_convert_str] = df[columns_to_convert_str].astype(str)

    logger.info("Filling missing values...")
    df.fillna({
        'category_code': 'Uncategorized',
        'brand': 'Unknown'
    }, inplace=True)

    logger.info("Dropping rows with null event_time or price...")
    df.dropna(subset=['event_time', 'price'], inplace=True)

    logger.info("Filtering out non-positive prices...")
    df = df[df['price'] > 0]

    logger.info("Dropping exact duplicates...")
    df.drop_duplicates(inplace=True)

    logger.info("Dropping duplicates based on user, product, and timestamp...")
    df['composite_col'] = (
        df['user_id'] + "_" +
        df['product_id'] + "_" +
        df['event_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    )
    df.drop_duplicates(subset='composite_col', inplace=True)
    df.drop(columns=['composite_col'], inplace=True)

    logger.info("Resetting index...")
    df.reset_index(drop=True, inplace=True)

    logger.info("Validating cleaned data...")
    validate_data(df)

    logger.info("Data cleaned. Final row count: %d", len(df))
    return df


def validate_data(df: pd.DataFrame):
    """
    Validates the cleaned DataFrame.
    """
    try:
        assert df['price'].min() > 0, "Non-positive prices exist"
        assert df['event_time'].notnull().all(), "Null event_time found"
        logger.info("Data validation: SUCCESS")
    except AssertionError as e:
        logger.error(f"Data validation: FAILED {str(e)}")
        raise


if __name__ == "__main__":
    init_logger()

    from extract import extract_csv
    raw_df = extract_csv('../data/raw/2019-Oct.csv', nrows=1000)
    clean_df = clean_data(raw_df)
    print(clean_df.info())
