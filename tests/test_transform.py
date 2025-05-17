import pandas as pd
from etl.transform import clean_data

def test_clean_data():
    # Sample raw data (some invalid)
    raw = pd.DataFrame({
        'price': [-10, 0, 50],
        'event_time': ['2020-01-01', '2020-01-01', '2020-01-01'],
        'event_type': ['view'] * 3,
        'user_id': ['u'] * 3,
        'product_id': ['p'] * 3,
        'category_id': [1] * 3,
        'category_code': ['appliance'] * 3,
        'brand': ['generic'] * 3
    })

    clean = clean_data(raw)

    # Test column types
    assert pd.api.types.is_datetime64_any_dtype(clean['event_time']), "event_time should be datetime"
    assert pd.api.types.is_numeric_dtype(clean['price']), "price should be numeric"
    assert pd.api.types.is_string_dtype(clean['product_id']), "product_id should be string"
    assert pd.api.types.is_string_dtype(clean['user_id']), "user_id should be string"

    # Filtered invalid prices
    assert (clean['price'] <= 0).sum() == 0, "Should remove rows with price <= 0"

    # Filtered nulls in key fields
    assert clean['price'].isnull().sum() == 0, "Should not contain null prices"
    assert clean['event_time'].isnull().sum() == 0, "Should not contain null event_time"

    # Filtered invalid event types
    assert clean['event_type'].isin(['view', 'cart', 'purchase']).all(), "Should only contain valid event types"

    # New columns created
    expected_cols = ['event_hour', 'event_date', 'category_code_1', 'category_code_2', 'category_code_3', 'category_code_4']
    for col in expected_cols:
        assert col in clean.columns, f"Missing column: {col}"
