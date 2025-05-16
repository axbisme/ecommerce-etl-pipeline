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
│ ├── raw/ # Raw input CSVs (ignored in Git)
│ └── clean/ # Cleaned, chunked output CSVs (ignored in Git)
├── etl/
│ ├── extract.py # Handles CSV extraction
│ ├── transform.py # Cleans and transforms raw data
│ └── load.py # (Coming soon) Loads clean data into PostgreSQL
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

