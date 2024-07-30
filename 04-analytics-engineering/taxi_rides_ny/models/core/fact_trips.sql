{{    config(
        materialized='table'
    )
}}

WITH green_cab_data AS (
    SELECT *,
        'Green' AS service_type
    FROM {{ ref('staging_green_cab_data') }}
),
yellow_cab_data AS (
    SELECT *,
        'Yellow' AS service_type
    FROM {{ ref('staging_yellow_cab_data') }}
),
trips_unioned as (
    SELECT * FROM green_cab_data
    UNION ALL
    SELECT * FROM yellow_cab_data
),
dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT  trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
FROM trips_unioned
INNER JOIN dim_zones as pickup_zone
ON trips_unioned.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones as dropoff_zone
ON trips_unioned.dropoff_locationid = dropoff_zone.locationid