## Exercises Repository
Author: Athina Zygogianni

Aviation Accidents ETL Pipeline (2000–2025)
This project implements a complete ETL (Extract – Transform – Load) pipeline for aviation accident data, scraped from aviation-safety.net and loaded into a PostgreSQL database using Docker & SQLAlchemy.
It collects, cleans, and stores structured accident records for further analytics, dashboards, and machine learning applications.

📌 Data Source
•	Website: https://aviation-safety.net
•	Dataset: Worldwide aviation accidents
•	Period: 2000 – 2025
•	Data includes:
o	Accident date
o	Aircraft type
o	Registration
o	Operator
o	Fatalities
o	Location
o	Aircraft damage
________________________________________
🧩 Components
1. Web Scraper
The scraper dynamically:
•	Detects the number of pages per year
•	Extracts accident tables
•	Normalizes column names
•	Adds metadata (year, page, source URL)
It saves the raw data to a CSV file:
asn_2000.csv
________________________________________
2. Data Transformation
Main transformations:
•	Accident_Date → converted to datetime
•	Fatalities → cleaned, converted to numeric, missing values removed
•	Aircraft_Damage standardized:
•	sub → Substantial
•	w/o → Destroyed, written off
•	min → Minor, repaired
•	non → None
•	unk → Unknown
All rows containing missing or inconsistent values are dropped to ensure data quality.
________________________________________
3. PostgreSQL Schema
The final table is created as plane 
fatalities.
________________________________________
4. Docker Setup
PostgreSQL runs in a Docker container.

____________________________________
🔗 Database Connection (from ETL container)
postgresql+psycopg2://postgres:postgres@db:5432/aviation_db
________________________________________
