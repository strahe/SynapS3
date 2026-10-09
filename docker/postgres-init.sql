-- Runs once, when the PostgreSQL data volume is first initialized. SynapS3
-- connects as a role that owns its database but is not a superuser.
\getenv app_password POSTGRES_APP_PASSWORD
CREATE ROLE synaps3 LOGIN PASSWORD :'app_password';
ALTER DATABASE synaps3 OWNER TO synaps3;
