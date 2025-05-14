# extract.py
import pandas as pd

def extract_csv(filepath: str, nrows: int = None) -> pd.DataFrame:
    """
    Extracts raw e-commerce data from a CSV file.
    """
    return pd.read_csv(filepath, nrows=nrows)

if __name__ == "__main__":
    df = extract_csv('../data/raw/2019-Oct.csv')
    print(df.head())
