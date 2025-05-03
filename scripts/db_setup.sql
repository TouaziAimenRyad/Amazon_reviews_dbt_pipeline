-- Create the schema and tables needed for the data warehouse
CREATE SCHEMA IF NOT EXISTS data_wearhouse;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create user for airflow if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'don') THEN
        CREATE USER don WITH PASSWORD 'donza3im';
    END IF;
END
$$;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE postgres_wearhouse TO don;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO don;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_wearhouse TO don;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO don;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO don;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_wearhouse TO don;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA analytics TO don;

-- Create function to handle timestamp conversion
CREATE OR REPLACE FUNCTION convert_amazon_timestamp(ts BIGINT) 
RETURNS TIMESTAMP AS $$
BEGIN
    -- Handle both second and millisecond timestamps
    IF ts > 1000000000000 THEN  -- Likely milliseconds
        RETURN to_timestamp(ts / 1000)::TIMESTAMP;
    ELSE
        RETURN to_timestamp(ts)::TIMESTAMP;
    END IF;
END;
$$ LANGUAGE plpgsql;