ALTER TABLE earthquakedw.time_dim
  ADD CONSTRAINT uniq_time_full_timestamp UNIQUE(full_timestamp);

ALTER TABLE earthquakedw.location_dim
  ADD CONSTRAINT uniq_loc_natural UNIQUE(latitude, longitude, nearest_locality, state, county, region);

ALTER TABLE earthquakedw.magnitude_type_dim
  ADD CONSTRAINT uniq_mag_natural UNIQUE(mag_type);

ALTER TABLE earthquakedw.data_source_dim
  ADD CONSTRAINT uniq_src_natural UNIQUE(net, mag_source);

ALTER TABLE earthquakedw.status_dim
  ADD CONSTRAINT uniq_status_natural UNIQUE(status, type);

-- 1) Time dimension
INSERT INTO earthquakedw.time_dim (full_timestamp, year, quarter, month, day)
SELECT 
    e.time
  , EXTRACT(YEAR   FROM e.time)::INT
  , EXTRACT(QUARTER FROM e.time)::INT
  , EXTRACT(MONTH  FROM e.time)::INT
  , EXTRACT(DAY    FROM e.time)::INT
  -- , EXTRACT(HOUR   FROM e.time)::INT
FROM reconciledDB.earthquake e
ON CONFLICT (full_timestamp) DO NOTHING
;

-- 2) Location dimension
INSERT INTO earthquakedw.location_dim (latitude, longitude, nearest_locality, state, county, region)
SELECT DISTINCT
    l.latitude
  , l.longitude
  -- , l.depth
  , l.nearest_locality
  , l.state
  , l.county
  , l.region
FROM reconciledDB.location l
ON CONFLICT (latitude, longitude,  nearest_locality, state, county, region) DO NOTHING
;

-- 3) Magnitude‐Type dimension
INSERT INTO earthquakedw.magnitude_type_dim (mag_type)
SELECT DISTINCT
    m.mag_type
  -- , m.mag
FROM reconciledDB.magnitude_detail m
ON CONFLICT (mag_type) DO NOTHING
;

-- 4) Data‐source dimension
INSERT INTO earthquakedw.data_source_dim (net, mag_source)
SELECT DISTINCT
    ds.net
  , ds.mag_source
FROM reconciledDB.data_source ds
ON CONFLICT (net, mag_source) DO NOTHING
;

-- 5) Status/Type dimension
INSERT INTO earthquakedw.status_dim (status, type)
SELECT DISTINCT
    e.status
  , e.type
FROM reconciledDB.earthquake e
ON CONFLICT (status, type) DO NOTHING
;

-- 6) Fact table
INSERT INTO earthquakedw.earthquake_fact (
    earthquake_id
  , time_id
  , location_id
  , magnitude_type_id
  , data_source_id
  , status_id
  , magnitude
  , depth
)
SELECT
    e.earthquake_id
  , t.time_id
  , loc_dim.location_id
  , mag_dim.magnitude_type_id
  , ds_dim.data_source_id
  , st_dim.status_id
  , e.magnitude
  , e.depth
FROM reconciledDB.earthquake e
  -- join the raw source tables to pull measures & keys
  JOIN reconciledDB.location       l  ON e.location_id        = l.location_id
  JOIN reconciledDB.magnitude_detail      m  ON e.magnitude_detail_id       = m.magnitude_detail_id
  JOIN reconciledDB.data_source    ds ON e.data_source_id     = ds.data_source_id
  -- map to dims
  JOIN earthquakedw.time_dim        t  ON t.full_timestamp     = e.time
  JOIN earthquakedw.location_dim    loc_dim
    ON loc_dim.latitude      = l.latitude
   AND loc_dim.longitude     = l.longitude
  --  AND loc_dim.depth         = l.depth
   AND loc_dim.nearest_locality = l.nearest_locality
   AND loc_dim.state         = l.state
   AND loc_dim.county        = l.county
   AND loc_dim.region        = l.region
  JOIN earthquakedw.magnitude_type_dim  mag_dim
    ON mag_dim.mag_type      = m.mag_type
  --  AND mag_dim.mag           = m.mag
  JOIN earthquakedw.data_source_dim ds_dim
    ON ds_dim.net           = ds.net
   AND ds_dim.mag_source    = ds.mag_source
  JOIN earthquakedw.status_dim     st_dim
    ON st_dim.status        = e.status
   AND st_dim.type          = e.type
ON CONFLICT (earthquake_id) DO NOTHING
;
