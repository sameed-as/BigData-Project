-- Create tables for the serving layer

CREATE TABLE crime_trends (
    id SERIAL PRIMARY KEY,
    year INT,
    month INT,
    day_of_week INT,
    hour_of_day INT,
    crime_count INT
);

CREATE TABLE arrest_rates (
    id SERIAL PRIMARY KEY,
    primary_type VARCHAR(255),
    district VARCHAR(50),
    race VARCHAR(100),
    arrest_rate FLOAT,
    total_crimes INT,
    total_arrests INT
);

CREATE TABLE violence_analysis (
    id SERIAL PRIMARY KEY,
    month INT,
    district VARCHAR(50),
    homicides INT,
    non_fatal_shootings INT
);

CREATE TABLE hotspots (
    id SERIAL PRIMARY KEY,
    cluster_id INT,
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE correlations (
    id SERIAL PRIMARY KEY,
    metric_x VARCHAR(255),
    metric_y VARCHAR(255),
    group_by VARCHAR(255),
    correlation_value FLOAT
);

CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    district VARCHAR(50),
    timestamp TIMESTAMP,
    event_count INT,
    threshold INT,
    alert_severity_level VARCHAR(50)
);
