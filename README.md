# E-Commerce ETL Pipeline

This project extracts, cleans, and transforms raw e-commerce clickstream data into an analytics-ready format. It's designed to handle large datasets efficiently using chunked processing.

## Features
- Chunked CSV extraction (memory-efficient)
- Modular data cleaning & transformation (with Pandas)
- Logging with progress tracking
- Output of clean CSV chunks ready for loading

## Project Structure

```bash
ecommerce-etl-pipeline/
├── data/
│ ├── raw/ # Raw input CSVs *ignored in Git
│ └── clean/ # Cleaned, chunked output CSVs *ignored in Git
├── etl/
│ ├── extract.py # Handles CSV extraction
│ ├── transform.py # Cleans and transforms raw data
│ └── load.py # Loads clean data into PostgreSQL
├── sql/ # Store useful SQL scripts 
│ ├── cart_to_purchase_conversion_rate.sql 
│ ├── view_to_cart_conversion_rate.sql 
│ └── view_to_purchase_conversion_rate.sql 
├── tests/ # Test cases for ETL pipeline
│ ├── test_extract.py # Test extraction
│ ├── test_load.py # Test loading
│ └── test_transform.py # Test clean and transform processes
├── docker-compose.yml
├── pipeline.py # Orchestrates ETL steps
├── requirements.txt
└── README.md
```

## Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/axbisme/ecommerce-etl-pipeline.git

cd ecommerce-etl-pipeline
```

### 2. Install dependencies 

```bash
pip install -r requirements.txt
```

### 3. Add raw CSV

The dataset used is Commerce Behavior Data from Multi-Category Store, available on Kaggle.

Download the dataset available [here](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) and place the file in the directory data/raw.

### 4. Run the pipeline

```bash
python3 pipeline.py
```

## Optional: Run test cases for the pipeline

```bash
PYTHONPATH=. pytest
```

## Data Successfully Loaded to PostgreSQL DB

The cleaned data has been loaded into a PostgreSQL database container using SQLAlchemy.

### Verify data loaded to DB

After running the pipeline, the `ecommerce_events` table was successfully created and populated:

```sql
-- Query to count event types
select event_type, count(*) total_cnt
  from public.ecommerce_events
 group by rollup(event_type)
 order by event_type;

 event_type |  total_cnt  
------------+-----------
 view       |     150121
 cart       |       3302
 purchase   |       3494
 [null]     |     156917

