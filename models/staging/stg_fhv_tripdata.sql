{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
)

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid
    ,dispatching_base_num
    ,cast(PULocationID as integer) as pickup_locationid
    ,cast(DOLocationID as integer) as dropoff_locationid
    
    -- timestamps
    ,cast(pickup_datetime as timestamp) as pickup_datetime
    ,cast(dropoff_datetime as timestamp) as dropoff_datetime
    
    -- trip info
    -- remove potential shared ride double counting ambiguity with Lyft records
    ,CASE
        WHEN dispatching_base_num IN ('B02510', 'B02844') THEN NULL
        ELSE cast(SR_Flag as integer)
    END as sr_flag
    ,Affiliated_base_number as affiliated_base_number

from tripdata
WHERE
    rn = 1

{% if var('is_test_run', default=true) %}

    LIMIT 100

{% endif %}
