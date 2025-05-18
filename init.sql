-- Create database first
CREATE DATABASE mydb;

-- Create user with replication privileges if it doesn't exist
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'cdc_user') THEN
      CREATE USER cdc_user WITH REPLICATION ENCRYPTED PASSWORD 'secret';
   END IF;
END
$do$;

-- Grant all privileges on database
GRANT ALL PRIVILEGES ON DATABASE mydb TO cdc_user;

-- Connect to mydb and grant schema privileges
\c mydb
GRANT ALL ON SCHEMA public TO cdc_user;
GRANT ALL ON ALL TABLES IN SCHEMA public TO cdc_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO cdc_user;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO cdc_user; 