-- Create catalog (if using Unity Catalog)
CREATE CATALOG IF NOT EXISTS gds_de_bootcamp;

-- Create schema
CREATE SCHEMA IF NOT EXISTS gds_de_bootcamp.default;

-- Note:
-- booking_fact and customer_scd will be created automatically by the ETL script
-- when df.write.saveAsTable(...) runs.
