import pandas as pd
from sqlalchemy import create_engine, text, inspect
from etl.load import load_data_to_db

# Connection string to your test PostgreSQL database
POSTGRES_URL = "postgresql://etl_user:etl_pass@localhost:5432/ecommerce_etl"
TEST_TABLE = "test_events"

def test_load_data_to_db():
    # Sample data to insert
    df = pd.DataFrame({
        'event_time': pd.to_datetime(['2020-01-01 10:00', '2020-01-01 11:00']),
        'event_date': ['2020-01-01', '2020-01-01'],
        'event_hour': [10, 11],
        'event_type': ['view', 'cart'],
        'product_id': ['p1', 'p2'],
        'user_id': ['u1', 'u2'],
        'price': [9.99, 19.99],
        'brand': ['nike', 'adidas'],
        'category_code_1': ['sports', 'sports'],
        'category_code_2': ['shoes', 'shoes'],
        'category_code_3': ['running', 'casual'],
        'category_code_4': ['men', 'women'],
    })

    # Load the data using your ETL function
    load_data_to_db(df, table_name=TEST_TABLE, connection_url=POSTGRES_URL)

    # Verify with SQLAlchemy
    engine = create_engine(POSTGRES_URL)
    with engine.connect() as conn:
        # Check row count
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TEST_TABLE}"))
        row_count = result.scalar()
        assert row_count == len(df), f"Expected {len(df)} rows, found {row_count}"

        # Optional: verify columns exist
        inspector = inspect(engine)
        actual_columns = [col["name"] for col in inspector.get_columns(TEST_TABLE)]
        for col in df.columns:
            assert col in actual_columns, f"Missing column: {col}"

    # Clean up: drop test table
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TEST_TABLE}"))
