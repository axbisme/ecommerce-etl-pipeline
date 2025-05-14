# transform.py

import logging
import pandas as pd

logger = logging.getLogger(__name__)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms raw e-commerce data.
    - Converts data types
    - Fills missing values
    - Drops invalid rows and duplicates
    - Validates cleaned dataset
    """
    logger.info("Begin data cleaning. Rows available: %d", len(df))
    initial_count = len(df)

    logger.info("Converting data types...")
    df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce', utc=True)
    df['price'] = pd.to_numeric(df['price'], errors='coerce').astype(float)
    df[['product_id', 'user_id', 'category_id']] = df[['product_id', 'user_id', 'category_id']].astype(str)

    logger.info("Filling missing values...")
    df.fillna({
        'category_code': 'Uncategorized',
        'brand': 'Unknown'
    }, inplace=True)

    logger.info("Dropping rows with null event_time or price...")
    df.dropna(subset=['event_time', 'price'], inplace=True)

    logger.info("Filtering out non-positive prices...")
    df = df[df['price'] > 0].copy()

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

    logger.info("Converting brand and category to lowercase...")
    df['category_code'] = df['category_code'].str.lower()
    df['brand'] = df['brand'].str.lower()

    logger.info("Dropping rows with unknown brand or uncategorized category...")
    df = df[df['category_code'] != 'uncategorized'].copy()
    df = df[df['brand'] != 'unknown'].copy()

    logger.info("Filtering valid event types...")
    df = df[df['event_type'].isin(['view', 'cart', 'purchase'])].copy()

    logger.info("Splitting category_code into subcategory columns...")
    category_split = df['category_code'].str.split(".", expand=True)
    expected_cols = ['category_code_1', 'category_code_2', 'category_code_3', 'category_code_4']
    for i, col in enumerate(expected_cols):
        df[col] = category_split[i] if i in category_split.columns else 'uncategorized'
    df.drop(columns=['category_code'], inplace=True)
    df.fillna(
        {col: 'uncategorized' for col in expected_cols},
        inplace=True
    )

    logger.info("Creating event_date and event_hour columns...")
    df['event_date'] = df['event_time'].dt.date
    df['event_hour'] = df['event_time'].dt.hour

    logger.info("Resetting index...")
    df.reset_index(drop=True, inplace=True)

    logger.info("Validating cleaned data...")
    validate_data(df)

    final_count = len(df)
    logger.info("Data cleaned. Final row count: %d", final_count)
    logger.info("Dropped %d rows during cleaning.", initial_count - final_count)

    logger.info("Reordering columns for consistency...")
    desired_order = [
        'event_time', 'event_date', 'event_hour', 'event_type', 'product_id',
        'user_id', 'price', 'brand', 'category_code_1', 'category_code_2',
        'category_code_3', 'category_code_4'
    ]
    df = df[desired_order]

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
        logger.error(f"Data validation: FAILED — {str(e)}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    from extract import extract_csv
    raw_df = extract_csv('../data/raw/2019-Oct.csv', nrows=10000)
    clean_df = clean_data(raw_df)
    clean_df.to_csv('../data/clean/2019-Oct-clean.csv', index=False)
    print(clean_df.info())
