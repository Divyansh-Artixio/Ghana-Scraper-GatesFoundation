-- Grant comprehensive permissions for Ghana FDA database
-- Run this as the database owner (divyanshsingh)

-- Grant schema usage permissions
GRANT USAGE ON SCHEMA safetydb TO PUBLIC;
GRANT USAGE ON SCHEMA public TO PUBLIC;

-- Grant table permissions for existing tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA safetydb TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO PUBLIC;

-- Grant sequence permissions
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA safetydb TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO PUBLIC;

-- Grant default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA safetydb GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO PUBLIC;

-- Grant default privileges for future sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA safetydb GRANT USAGE, SELECT ON SEQUENCES TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO PUBLIC;

-- Grant specific permissions to known users
DO $$
BEGIN
    -- Grant to fda_user if exists
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'fda_user') THEN
        GRANT USAGE ON SCHEMA safetydb TO fda_user;
        GRANT USAGE ON SCHEMA public TO fda_user;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA safetydb TO fda_user;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fda_user;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA safetydb TO fda_user;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fda_user;
    END IF;
    
    -- Grant to regulatory_user if exists
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'regulatory_user') THEN
        GRANT USAGE ON SCHEMA safetydb TO regulatory_user;
        GRANT USAGE ON SCHEMA public TO regulatory_user;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA safetydb TO regulatory_user;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO regulatory_user;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA safetydb TO regulatory_user;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO regulatory_user;
    END IF;
    
    -- Grant to postgres superuser
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'postgres') THEN
        GRANT USAGE ON SCHEMA safetydb TO postgres;
        GRANT USAGE ON SCHEMA public TO postgres;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA safetydb TO postgres;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postgres;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA safetydb TO postgres;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;
    END IF;
END
$$;

-- Show current permissions
SELECT 
    schemaname,
    tablename,
    tableowner,
    hasinsert,
    hasselect,
    hasupdate,
    hasdelete
FROM pg_tables 
WHERE schemaname IN ('safetydb', 'public')
ORDER BY schemaname, tablename;

-- Show schema permissions
SELECT 
    nspname as schema_name,
    nspowner::regrole as owner
FROM pg_namespace 
WHERE nspname IN ('safetydb', 'public');

PRINT 'Permissions granted successfully to all users for safetydb and public schemas!';