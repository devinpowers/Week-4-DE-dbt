{{ config(materialized = 'view')}}

-- source(name, table_name)
-- this is in our schema.yml file and the name staging can be anything we want
SELECT * FROM {{ source('staging', 'green_tripdata')}}

LIMIT 100
