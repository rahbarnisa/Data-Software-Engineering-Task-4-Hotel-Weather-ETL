# Restaurant Weather ETL Pipeline

## Project Overview

This project implements an ETL pipeline for processing restaurant and weather datasets using Apache Spark.

The pipeline performs the following tasks:

* Reads restaurant data from local CSV files.
* Reads weather data from local Parquet files.
* Detects missing latitude and longitude values.
* Retrieves missing coordinates using the OpenCage Geocoding API.
* Generates a 4-character geohash based on latitude and longitude.
* Joins restaurant and weather datasets using geohash.
* Stores the enriched dataset locally in Parquet format.
* Includes unit tests and automation scripts.

## Technologies Used

* Python 3.11
* Apache Spark (PySpark)
* OpenCage Geocoding API
* Geohash2
* PyTest
* Git

## Project Structure

spark-restaurant-weather-etl/

├── data/

│ ├── input/

│ │ ├── restaurants/

│ │ └── weather/

│ └── output/

├── src/

│ └── restaurant_weather_etl.py

├── tests/

│ └── test_etl.py

├── run.bat

├── requirements.txt

├── .gitignore

└── README.md

## Installation

Install dependencies:

pip install -r requirements.txt

## Running the Pipeline

python src/restaurant_weather_etl.py

or

run.bat

## Running Tests

pytest

## Output

The resulting enriched dataset is stored in:

data/output/enriched_restaurant_weather

## Features Implemented

 Restaurant data ingestion

 Weather data ingestion

 Missing coordinate detection

 OpenCage API integration

 Geohash generation

 Data enrichment

 Parquet output generation

 Unit testing

## Author

Rahbarnisa Yusupova
