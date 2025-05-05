-- Create database first (if not exists)
SELECT 'CREATE DATABASE postgres_wearhouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'postgres_wearhouse')\gexec

-- Create the schema and tables needed for the data warehouse
CREATE SCHEMA IF NOT EXISTS data_wearhouse;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Then create user
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'don') THEN
        CREATE USER don WITH PASSWORD 'donza3im';
    END IF;
END
$$;

-- Then grant privileges
GRANT ALL PRIVILEGES ON DATABASE postgres_wearhouse TO don;