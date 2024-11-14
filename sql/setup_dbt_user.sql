-- Create the `transform` role
DO $$ BEGIN
    CREATE ROLE transform;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;


-- Create our database and schemas
DO $$ BEGIN
    CREATE DATABASE airbnb;
EXCEPTION
    WHEN duplicate_database THEN NULL;
END $$;

\c airbnb

-- Create the `dbt` user and assign to role
DO $$ BEGIN
    CREATE USER dbt WITH PASSWORD 'dbtPassword123';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

ALTER USER dbt SET search_path TO 'airbnb', 'public';
GRANT transform TO dbt;


DO $$ BEGIN
    CREATE SCHEMA raw;
EXCEPTION
    WHEN duplicate_schema THEN NULL;
END $$;

-- Set up permissions to role `transform`
GRANT ALL PRIVILEGES ON DATABASE airbnb TO transform;
GRANT ALL PRIVILEGES ON SCHEMA raw TO transform;


SET search_path TO raw;


-- Create our three tables and import the data from CSV files
CREATE TABLE IF NOT EXISTS raw_listings (
    id SERIAL PRIMARY KEY,
    listing_url TEXT,
    name TEXT,
    room_type TEXT,
    minimum_nights INTEGER,
    host_id INTEGER,
    price TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

COPY raw_listings (id, listing_url, name, room_type, minimum_nights, host_id, price, created_at, updated_at)
FROM '/var/data/listings.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE IF NOT EXISTS raw_reviews (
    listing_id INTEGER,
    date TIMESTAMP,
    reviewer_name TEXT,
    comments TEXT,
    sentiment TEXT
);

COPY raw_reviews (listing_id, date, reviewer_name, comments, sentiment)
FROM '/var/data/reviews.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE IF NOT EXISTS raw_hosts (
    id SERIAL PRIMARY KEY,
    name TEXT,
    is_superhost TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

COPY raw_hosts (id, name, is_superhost, created_at, updated_at)
FROM '/var/data/hosts.csv' DELIMITER ',' CSV HEADER;