-- Connect to the new database (psql meta‑command)




-- 3) Location table
CREATE TABLE reconciledDB.location (
  location_id  SERIAL          PRIMARY KEY,
  offset_distance  DOUBLE PRECISION NOT NULL,
  offset_direction    TEXT             NOT NULL,
  nearest_locality       TEXT             NOT NULL,
  state              TEXT             NOT NULL,
  county             TEXT             NOT NULL,
  region             TEXT             NOT NULL,
  latitude         DOUBLE PRECISION NOT NULL,
  longitude        DOUBLE PRECISION NOT NULL,
  -- depth            DOUBLE PRECISION NOT NULL,
  horizontal_error DOUBLE PRECISION NOT NULL,
  depth_error      DOUBLE PRECISION NOT NULL
);

-- 4) Magnitude table
CREATE TABLE reconciledDB.magnitude_detail (
  magnitude_detail_id  SERIAL          PRIMARY KEY,
  mag_type    TEXT             NOT NULL,
  mag_error   DOUBLE PRECISION NOT NULL,
  mag_nst     INTEGER          NOT NULL
);

-- 5) SeismicMetrics table
CREATE TABLE reconciledDB.seismic_metrics (
  seismic_metrics_id  SERIAL          PRIMARY KEY,
  nst       INTEGER          NOT NULL,
  gap       DOUBLE PRECISION NOT NULL,
  dmin      DOUBLE PRECISION NOT NULL,
  rms       DOUBLE PRECISION NOT NULL
);

-- 6) DataSource table
CREATE TABLE reconciledDB.data_source (
  data_source_id  SERIAL          PRIMARY KEY,
  net             TEXT             NOT NULL,
  location_source TEXT             NOT NULL,
  mag_source      TEXT             NOT NULL
);


-- 1) Core earthquake table
CREATE TABLE reconciledDB.earthquake (
  earthquake_id            TEXT         PRIMARY KEY,
  time          TIMESTAMPTZ  NOT NULL,
  updated       TIMESTAMPTZ  NOT NULL,
  status        TEXT         NOT NULL,
  type          TEXT         NOT NULL,
  magnitude         DOUBLE PRECISION NOT NULL,
  depth            DOUBLE PRECISION NOT NULL,
  location_id     INTEGER       NOT NULL REFERENCES reconciledDB.location(location_id),
  magnitude_detail_id     INTEGER       NOT NULL REFERENCES reconciledDB.magnitude_detail(magnitude_detail_id),
  seismic_metrics_id     INTEGER       NOT NULL REFERENCES reconciledDB.seismic_metrics(seismic_metrics_id),
  data_source_id       INTEGER       NOT NULL REFERENCES reconciledDB.data_source(data_source_id)
);

