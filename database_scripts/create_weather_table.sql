CREATE TABLE IF NOT EXISTS weather (
    country VARCHAR(50) NOT null distkey,
    location_name VARCHAR(50) NOT NULL,
    temperature FLOAT NOT NULL,
    wind_speed FLOAT NOT NULL,
    wind_direction VARCHAR(4) NOT NULL,
    pressure FLOAT NOT NULL,
    humidity INT NOT NULL,
    cloud INT NOT NULL,
    feels_like FLOAT NOT NULL,
    visibility FLOAT NOT NULL,
    last_updated DATETIME NOT NULL
) sortkey(temperature);