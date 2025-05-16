from etl.extract import extract_csv
import pandas as pd

def test_extract_csv():
    df = extract_csv('data/raw/2019-Oct.csv', nrows=10)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 10;