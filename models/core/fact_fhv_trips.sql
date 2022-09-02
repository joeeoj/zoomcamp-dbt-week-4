{{ config(materialized='table') }}

WITH dim_zones as (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT
    f.tripid
    ,dispatching_base_num
    ,'FHV' as service_type
    ,f.pickup_datetime
    ,f.pickup_locationid
    ,pickup_zone.borough as pickup_borough
    ,pickup_zone.zone as pickup_zone

    ,f.dropoff_datetime
    ,f.dropoff_locationid
    ,dropoff_zone.borough as dropoff_borough
    ,dropoff_zone.zone as dropoff_zone

    ,f.sr_flag
    ,f.affiliated_base_number

FROM {{ ref('stg_fhv_tripdata') }} f
INNER JOIN dim_zones as pickup_zone
    ON f.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones as dropoff_zone
    ON f.dropoff_locationid = dropoff_zone.locationid
