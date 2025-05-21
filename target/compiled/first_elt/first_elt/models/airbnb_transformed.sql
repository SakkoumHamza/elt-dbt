with source as (
    select * from "elt_project"."public"."airbnb_data"
),

cleaned as (
    select
        name,
        cast("host id" as bigint) as host_id,
        coalesce(cast("host_identity_verified" as varchar(25)), 'unconfirmed') as is_host_verified ,-- No need to COALESCE again
        cast("host name" as varchar(25)) as host_name,
        cast("neighbourhood group" as varchar(50)) as neighborhood_group,
        cast(neighbourhood as varchar(100)) as neighborhood,
        cast(lat as numeric(9,6)) as latitude,
        cast(long as numeric(9,6)) as longitude, -- Renamed
        coalesce(instant_bookable, false) as is_instant_bookable,  -- Replaced NULL with false (boolean column)
        cast(cancellation_policy as varchar(50)) as cancellation_policy,
        cast("room type" as varchar(50)) as room_type,
        cast(cast("Construction year" as numeric )as integer) as construction_year,
        cast(replace(replace(price, ',', ''), '$', '') as INTEGER) as price,
        cast(coalesce(replace(replace("service fee", ',', ''), '$', ''), '0') as INTEGER) as service_fee,
        case
            when cast("minimum nights" as integer ) <0 then 1
            else coalesce(cast("minimum nights" as integer), 1) -- Replaced NULL with 1
        end as minimum_nights,
        coalesce(cast("number of reviews" as integer), 0) as review_count,  -- Replaced NULL with 0
        coalesce(cast("review rate number" as INTEGER ) ,0) as review_rating,  -- Replaced NULL with 0
        coalesce(cast("calculated host listings count" as integer), 1) as host_listings_count,  -- Replaced NULL with 1
        case
            when cast("availability 365"  as integer ) < 0 then 1
            when cast("availability 365"  as integer )  > 365 then 365
            else coalesce(cast("availability 365"  as integer ) , 1)
        end as availability_365,
        regexp_replace(coalesce(house_rules, 'No rules'), '^[^a-zA-Z]+', '') as house_rules -- Symbols at the Beginning removed and null values replaced
    from source
    where 
        name is not null
        and price is not null
        and cancellation_policy is not null
        and "neighbourhood group" is not null
        and neighbourhood is not null
        and "room type" is not null
        and "host name" is not null
        and "Construction year" is not null
        and "host id" is not null
        and lat is not null
        and long is not null
)

select * from cleaned