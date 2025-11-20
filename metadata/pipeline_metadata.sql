
-- Create metadata table to track last processed date
CREATE TABLE IF NOT EXISTS gds_de_bootcamp.default.pipeline_metadata (
    table_name STRING,
    last_processed_date DATE
);

-- Insert initial record if no entry exists
INSERT INTO gds_de_bootcamp.default.pipeline_metadata
VALUES ("booking_customer_pipeline", DATE("1900-01-01"));
