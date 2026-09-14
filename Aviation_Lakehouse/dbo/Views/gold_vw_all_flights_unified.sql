CREATE   VIEW dbo.gold_vw_all_flights_unified AS

-- 1. Batch Historical Facts (5M+ Historical Records)
SELECT 
    f.flight_number,
    f.airline_code,
    a.airline_name,
    f.origin_airport_code,
    orig.city AS origin_city,
    f.dest_airport_code,
    dest.city AS dest_city,
    f.departure_delay_mins,
    f.is_delayed_flag,
    f.is_cancelled_flag,
    'HISTORICAL_BATCH' AS data_source
FROM dbo.gold_fact_flights f
LEFT JOIN dbo.gold_dim_airlines a ON f.airline_code = a.airline_code
LEFT JOIN dbo.gold_dim_airports orig ON f.origin_airport_code = orig.airport_code
LEFT JOIN dbo.gold_dim_airports dest ON f.dest_airport_code = dest.airport_code

UNION ALL

-- 2. Live Streaming Telemetry (Real-Time Records)
SELECT 
    s.flight_number,
    s.airline_code,
    a.airline_name,
    s.origin_airport_code,
    orig.city AS origin_city,
    s.dest_airport_code,
    dest.city AS dest_city,
    s.departure_delay_mins,
    s.is_delayed_flag,
    s.is_cancelled_flag,
    'LIVE_STREAM' AS data_source
FROM dbo.silver_live_telemetry s
LEFT JOIN dbo.gold_dim_airlines a ON s.airline_code = a.airline_code
LEFT JOIN dbo.gold_dim_airports orig ON s.origin_airport_code = orig.airport_code
LEFT JOIN dbo.gold_dim_airports dest ON s.dest_airport_code = dest.airport_code;

GO