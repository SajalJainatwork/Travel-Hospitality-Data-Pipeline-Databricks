# travel-hospitality-data-pipeline-databricks
End-to-end Databricks ETL Pipeline using Medallion Architecture, Delta Lake, PySpark, Metadata-driven Incremental Ingestion, and SCD2 Customer Dimension.


This project builds a real-world automated data pipeline that processes daily customer and booking CSV files using Databricks, PySpark, Delta Lake, and Unity Catalog.

The system works like a real company’s data platform, where new files arrive every day and must be processed accurately, automatically, and without missing anything.

💡 What the Pipeline Does
1️⃣ Automatically Detects & Processes New Files

A metadata table remembers the last processed date.

Every time the pipeline runs, it picks only new or late-arriving files.

No duplicate runs, no missed data.

2️⃣ Cleans Data & Performs Data Quality Checks

Before loading anything:

Checks for missing values

Ensures uniqueness

Validates numbers

Ensures row counts are correct

If something looks wrong → pipeline stops immediately.

3️⃣ Tracks Customer History with SCD Type 2

If customer details change (name, address, etc.):

Old record is closed

New record is inserted

History is fully preserved

This lets analysts see what the customer looked like at any point in time.

4️⃣ Builds a Gold-Level Fact Table

Daily bookings are combined and summarized:

Total revenue

Total quantity

Per-customer metrics

Stored as a clean Delta table ready for reports and dashboards.

5️⃣ Completely Automated with Databricks Workflows

Runs every day

No manual input

Uses metadata to decide what to ingest

Sends alerts on failures

Production-grade 🚀

🏗 Architecture (Bronze → Silver → Gold)

Bronze → Raw daily CSVs

Silver → Cleaned + validated + SCD2 customer table

Gold → Final aggregated booking facts

Follows the Medallion Architecture used in modern data engineering.

📁 What’s Included

Notebook for processing

Metadata SQL

Sample data

Scripts to create Delta tables

Architecture diagram

🎯 Final Output

booking_fact (Gold)

customer_scd (Silver)

pipeline_metadata (For tracking processing)
