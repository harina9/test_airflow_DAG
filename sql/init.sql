CREATE TABLE raw_weather (
  id SERIAL PRIMARY KEY,
  date_weather TIMESTAMP NOT NULL,
  temp_weather FLOAT NOT NULL
);

CREATE TABLE averg_temp (
  id SERIAL PRIMARY KEY,
  state VARCHAR NOT NULL,
  avg_temp FLOAT NOT NULL
);
