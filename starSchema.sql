-- 1) Time Dimension
CREATE TABLE earthquakeDW.time_dim (
  time_id        SERIAL        PRIMARY KEY,
  full_timestamp TIMESTAMPTZ  NOT NULL,
  year           INTEGER       NOT NULL,
  quarter        INTEGER       NOT NULL CHECK (quarter BETWEEN 1 AND 4),
  month          INTEGER       NOT NULL CHECK (month BETWEEN 1 AND 12),
  day            INTEGER       NOT NULL CHECK (day BETWEEN 1 AND 31),
  -- hour           INTEGER       NULL           CHECK (hour BETWEEN 0 AND 23)
);

-- 2) Location Dimension
CREATE TABLE earthquakeDW.location_dim (
  location_id  SERIAL          PRIMARY KEY,
  latitude     DOUBLE PRECISION NOT NULL,
  longitude    DOUBLE PRECISION NOT NULL,
  -- depth        DOUBLE PRECISION NOT NULL,
  nearest_locality       TEXT    NOT NULL,
  state       TEXT            NOT NULL,
  county      TEXT            NOT NULL,
  region      TEXT            NOT NULL

);

-- 3) Magnitude‑Type Dimension
CREATE TABLE earthquakeDW.magnitude_type_dim (
  magnitude_type_id  SERIAL          PRIMARY KEY,
  mag_type     TEXT            NOT NULL
  -- mag		   DOUBLE PRECISION NOT NULL
);

-- 4) Source Dimension
CREATE TABLE earthquakeDW.data_source_dim (
  data_source_id    SERIAL          PRIMARY KEY,
  net          TEXT            NOT NULL,
  mag_source   TEXT            NOT NULL
);

-- 5) Status/Type Dimension
CREATE TABLE earthquakeDW.status_dim (
  status_id SERIAL        PRIMARY KEY,
  status         TEXT          NOT NULL,
  type           TEXT          NOT NULL
);


-- 7) Fact Table: Earthquake Events
CREATE TABLE earthquakeDW.earthquake_fact (
  earthquake_id        TEXT          PRIMARY KEY,
  time_id         INTEGER       NOT NULL REFERENCES earthquakeDW.time_dim(time_id),
  location_id     INTEGER       NOT NULL REFERENCES earthquakeDW.location_dim(location_id),
  magnitude_type_id     INTEGER       NOT NULL REFERENCES earthquakeDW.magnitude_type_dim(magnitude_type_id),
  data_source_id       INTEGER       NOT NULL REFERENCES earthquakeDW.data_source_dim(data_source_id),
  status_id  INTEGER       NOT NULL REFERENCES earthquakeDW.status_dim(status_id),

  -- Measures
  magnitude       DOUBLE PRECISION NOT NULL,
  depth           DOUBLE PRECISION NOT NULL
);
