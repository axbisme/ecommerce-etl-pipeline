import pandas as pd
from etl.transform import clean_data

def test_clean_data_filters_invalid_rows():
    raw = pd.DataFrame({'price': [-10, 0, 50], 'event_time': ['2020-01-01', '2020-01-01', '2020-01-01'],
                        'event_type': ['view']*3, 'user_id': ['u']*3, 'product_id': ['p']*3, 'category_id': [1]*3,
                        'category_code': ['appliance']*3, 'brand' : ['generic']*3})
    clean = clean_data(raw)
    
    assert pd.api.types.is_datetime64_any_dtype(clean['event_time'])
    assert pd.api.types.is_numeric_dtype(clean['price'])
    assert pd.api.types.is_string_dtype(clean['product_id'])
    assert pd.api.types.is_string_dtype(clean['user_id'])
    assert (clean['price'] <= 0).sum() == 0