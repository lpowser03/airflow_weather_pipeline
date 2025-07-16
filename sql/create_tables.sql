CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    collected_at TIMESTAMP UNIQUE NOT NULL,
    temperature DECIMAL(5,2),
    humidity INTEGER,
    pressure DECIMAL(7,2),
    wind_speed DECIMAL(5,2),
    weather_condition VARCHAR(50),
    visibility INTEGER,
    city VARCHAR(50) DEFAULT 'San Francisco'
);