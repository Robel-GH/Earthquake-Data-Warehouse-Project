
BEGIN;

-- 1) (Optional) Add UNIQUE constraints on each dimension so you can safely use ON CONFLICT
ALTER TABLE reconciledDB.location
  ADD CONSTRAINT location_natural_key UNIQUE (
    offset_distance,
    offset_direction,
    nearest_locality,
    state,
    county,
    region,
    latitude,
    longitude,
    horizontal_error,
    depth_error
  );

ALTER TABLE reconciledDB.magnitude_detail
  ADD CONSTRAINT magnitude_natural_key UNIQUE (
    mag_type,
    mag_error,
    mag_nst
  );

ALTER TABLE reconciledDB.seismic_metrics
  ADD CONSTRAINT seismic_metrics_natural_key UNIQUE (
    nst,
    gap,
    dmin,
    rms
  );

ALTER TABLE reconciledDB.data_source
  ADD CONSTRAINT data_source_natural_key UNIQUE (
    net,
    location_source,
    mag_source
  );

-- 2) Load Location dimension
INSERT INTO reconciledDB.location (
  offset_distance,
  offset_direction,
  nearest_locality,
  state,
  county,
  region,
  latitude,
  longitude,
  horizontal_error,
  depth_error
)
SELECT DISTINCT
  s.offset_distance::DOUBLE PRECISION,
  s.offset_direction,
  s.nearest_locality,
  s.state,
  s.county,
  s.region,
  s.latitude,
  s.longitude,
  s.horizontalError,
  s.depthError
FROM staging_events s
ON CONFLICT ON CONSTRAINT location_natural_key DO NOTHING;

-- 3) Load Magnitude dimension
INSERT INTO reconciledDB.magnitude_detail (
  mag_type,
  mag_error,
  mag_nst
)
SELECT DISTINCT
   s.magType,
  s.magError,
  s.magNst::INTEGER
FROM staging_events s
ON CONFLICT ON CONSTRAINT magnitude_natural_key DO NOTHING;

-- 4) Load SeismicMetrics dimension
INSERT INTO reconciledDB.seismic_metrics (
  nst,
  gap,
  dmin,
  rms
)
SELECT DISTINCT
  s.nst::INTEGER,
  s.gap,
  s.dmin,
  s.rms
FROM staging_events s
ON CONFLICT ON CONSTRAINT seismic_metrics_natural_key DO NOTHING;

-- 5) Load DataSource dimension
INSERT INTO reconciledDB.data_source (
  net,
  location_source,
  mag_source
)
SELECT DISTINCT
  s.net,
  s.locationSource,
  s.magSource
FROM staging_events s
ON CONFLICT ON CONSTRAINT data_source_natural_key DO NOTHING;

-- 6) Populate the Earthquake fact table by joining back to each dimension
INSERT INTO reconciledDB.earthquake (
  earthquake_id,
  time,
  updated,
  status,
  type,
  magnitude,
  depth,
  location_id,
   magnitude_detail_id,
  seismic_metrics_id,
  data_source_id
)
SELECT
  s.earthquake_id,
  s.time,
  s.updated,
  s.status,
  s.type,
  s.mag,
  s.depth,
  l.location_id,
  m. magnitude_detail_id,
  sm.seismic_metrics_id,
  ds.data_source_id
FROM staging_events s
  JOIN reconciledDB.location l
    ON l.offset_distance   = s.offset_distance::DOUBLE PRECISION
   AND l.offset_direction  = s.offset_direction
   AND l.nearest_locality  = s.nearest_locality
   AND l.state             = s.state
   AND l.county            = s.county
   AND l.region            = s.region
   AND l.latitude          = s.latitude
   AND l.longitude         = s.longitude
  --  AND l.depth             = s.depth
   AND l.horizontal_error  = s.horizontalError
   AND l.depth_error       = s.depthError
  JOIN reconciledDB.magnitude_detail m
    -- ON m.mag       = s.mag
   ON m.mag_type  = s.magType
   AND m.mag_error = s.magError
   AND m.mag_nst   = s.magNst::INTEGER
  JOIN reconciledDB.seismic_metrics sm
    ON sm.nst = s.nst::INTEGER
   AND sm.gap = s.gap
   AND sm.dmin = s.dmin
   AND sm.rms = s.rms
  JOIN reconciledDB.data_source ds
    ON ds.net             = s.net
   AND ds.location_source = s.locationSource
   AND ds.mag_source      = s.magSource
ON CONFLICT (earthquake_id) DO NOTHING;

COMMIT;
