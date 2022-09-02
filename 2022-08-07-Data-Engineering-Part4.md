---
title: 'Zoom Data Engineering Part 4'

header:
  image: "/images/chicagotwo.jpeg"

date: 2022-07-23

toc: true
toc_label: "Table of Contents" 


tags:
  - Data Engineering
  - Tutorials
  - SQL
  - Database
  - dbt
---
## Project and Scope of this Week

**Goal of Proejct:** Transforming the data loaded in our Data Warehouse (BigQuery) to Analytical Views (Google) bying using a transformation tool called **dbt**.

## Prerequisites 

By this *stage of the course* you should have already: 

- A running Data Warehouse (BigQuery) 
- A set of running pipelines ingesting the project dataset:
    * Yellow taxi data - Years 2019 and 2020 [x]
    * Green taxi data - Years 2019 and 2020  [x]
    * fhv data - Year 2019. [x]



## What is Analytics Engineering? And how do they fit in?

The **analytics engineer** is the role that tries to fill the gap: it introduces the good software engineering practices to the efforts of data analysts and data scientists. The analytics engineer may be exposed to the following tools:

1. Data Loading (Stitch...)

2. Data Storing (Data Warehouses)

3. Data Modeling (dbt, Dataform...)

4. Data Presentation (BI tools like Looker, Mode, Tableau...)


## Data Modeling Concepts

In this lesson we will cover the T in ETL, which is **transformation**



## Dimensional Modeling


* Fact and Dimensional tables (STAR Method)



## Understanding the **architecture** of Dimensional Modeling:


**Stage Area:**
  * Contains the raw data, which is not meant to be exposed to everyone


**Processing area:**
  From raw data to data models, focues on the efficiency and ensuring standards


**Presentation area:**
  Final presentation of the data, aka the exposure to the business stakeholder




## Introduction to dbt

**START OF WEEK 4!!!**

### Scope of Project (using dbt)

Our project will have trip data (which we've loaded into BigQuery). We'll add a CSV file with the taxi lookup data. We'll use DBT to transform our data in BigQuery, and then express our data within a dashboard.


dbt has 2 main components: dbt Core and dbt Cloud:

**dbt Core:** open-source project that allows the data transformation.
  Builds and runs a dbt project (.sql and .yaml files).
  Includes SQL compilation logic, macros and database adapters.
  Includes a CLI interface to run dbt commands locally.
  Open-source and free to use.

**dbt Cloud:** SaaS application to develop and manage dbt projects.
  Web-based IDE to develop, run and test a dbt project.
  Jobs orchestration.
  Logging and alerting.
  Intregrated documentation.
  Free for individuals (one developer seat).

For integration with **BigQuery** we will use the **dbt Cloud IDE**, so a local installation of dbt core isn't required.

![](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/images/04_02.png)

### What is dbt?

**dbt** is used for **transformation**
  * Transformation tool

### Models in dbt

What are Models in dbt?

Models are **snippets** of SQL code that help **shape** data into a format that will be ready for reporting (Analytics/Business Intelligence)

  * Models live in the Models directory of a dbt projecty
  * Each Model has a One-to-One relationship with a table or view in the database
  * You can run all the models, dbt will decide which/what models to build in-order by running the command `dbt run`


At the *top* of our Model file we can include a Configuration Block that allows us to tell dbt to build either a **Table** or a **View**. An example of the code snippet block to config for a table is shown below:

```sql
{{ config(materialized='table') }}
```

If you want to run a specific model you can do so by running:

`dbt run -m specifc_model`

After running the Model you can check into your Data Warehouse (BigQuery, Snowflake, etc) and see that dbt built the modell


### Naming Conventions for Models

**Modularity:** Build *Part-by-Part* and *assemble** together. Think of building a car.

**Sources:** Raw data that has been loaded. (Not Models), they a way of referencing. ()



**Staging:** Clean and standardize the data (One-to-One with the source tables).

**Itermediate Models:** Exist somewhere between staging and final models. They should always reference staging models

**Note:** dbt docs at the to left of the project we can click on `view docs` and dbt will generate documentation for us.

### Sources

**What are Sources in dbt?**

Sources in **dbt** allow you to document those raw tables that have been brought in by EL tools (Extract, Load) you configure the source **once** in a .yml file. A lot like the *ref function* we used with models.

For Example if location or naming of raw tables change you need to update across multiple files


**Source Function Example:**

```sql
{{source(’stripe’, ‘payments’)}} 
```
This is creating a direct reference to that .yml file.

When we compile our code:

```sql
{{source(’stripe’, ‘payments’)}}
```
compiles to 

```sql
raw.stripe.payments
```

So Sources are useful becuase when the Loading process changes you can update your dbt project in one place and everything can be up running again. 


### Tests in dbt

dbt tests are *assertion* you have about your data that are correct.

Testing at scale is difficult, run in dbt:

```sql
dbt test
```

Two types of tests in dbt which are singular and generic.

**Singular Tests**

- Very specific, maybe apply to 1 or 2 models and they *assert* something really specific about the logic in the model

**Generic Tests**

- Generic Tests are simple generic logic that can be used in your project

- 4 Types that come with dbt
    - **Unique**:  Every value in column is unique
    - **not_null**: Every value in the column is not null
    - **accepted_values**: Every value in the colmun is a value from a given list
    - **relationships**:  Every value in the colmun exisits in the column of another table

**Note:** Additional testing can be imported through packages OR write your custom generic tests.


## Steps to use dbt in Our Project (START HERE)



### Step 0: Prereq

Setting/making sure that our BigQuery Database has everything 'correctly' set up for this part!!!


### Step 1: Set up dbt and dbt Cloud

The first step is to create a user account in [**dbt**](https://www.getdbt.com/) and create a project and connect it to a database. 

Since we will be using **BigQuery** as our warehouse for this project we will use **dbt cloud** instead of *dbt core*, which is for developing on a local machine.

We will connect dbt to BigQuery using [BigQuery OAuth](https://docs.getdbt.com/docs/dbt-cloud/cloud-configuring-dbt-cloud/cloud-setting-up-bigquery-oauth).

Make sure that you have set up a **GitHub repo** for this part of the [project](https://github.com/devinpowers/DE-Week4).


SHOULD LINK GITHUB AND DBT TOGETHER!!!!

Click on init in dbt and it will create all the folders needed (e.g. Models, Seeds, tests, snapshots, etc.)


Change the **name** of the project in the `dbt_project.yml` file to:

`name: 'taxi_ride_ny'`

and change:

`models:` to `taxi_ride_ny`





### dbt Core

* For running **dbt** locally on your machine.


<iframe width="560" height="315" src="https://www.youtube.com/embed/UVI30Vxzd6c" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>


### Developing with dbt 


**Define a Source and Create First Model**

We will create two folders inside the `Models` folder in dbt. One file will be called `staging` and the other will be called `core`.

Inside the `staging` folder we will create a (file) model named `stg_green_tripdata.sql`


Were going to create a `schema.yml` file inside the `staging` folder were we can define the **source**, here is the `schema.yml` below:


**Note:**

  * `name` we can name this whatever, just **staging** in this example
  * `database` This is from BigQuery
  * `schema` This is also from BigQuery

  * `tables` 

```yml
version: 2

sources:
    - name: staging
      database: new-try-zoom-data
      schema: trips_data_all

      tables:
        - name: green_tripdata
        - name: yellow_tripdata
```



In the model below called `stg_green_tripdata.sql` were going to define our *View*:

```sql
{{ config(materialized = 'view')}}

-- source(name, table_name)
-- this is in our schema.yml file and the name staging can be anything we want
SELECT * FROM {{ source('staging', 'green_tripdata')}}

LIMIT 100
```

We can run ouir model in dbt by using the command:

```bash
dbt run
```

And it executed!

Lets edit the `stg_green_tripdata.sql` and add on to the model:


```sql
{{ config(materialized='view') }}


select
    -- identifiers
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    cast(congestion_surcharge as numeric) as congestion_surcharge
from {{source ('staging', 'green_tripdata')}}
limit 100   
```

Can run with either  `dbt run` or run our model by itself `dbt run --select stg_green_tripdata`



## Defintion and Usage of Macros


Under the `macro` folder we will add a new file called `get_payment_type_description.sql`

```sql
 {#
    This macro returns the description of the payment_type 
#}

{% macro get_payment_type_description(payment_type) -%}

    case {{ payment_type }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end

{%- endmacro %}
```


And then we go to the `stg_green_tripdata.sql` and call the macro:

Add this:

```sql
{{ get_payment_type_description('payment_type') }} as payment_type_description,
```

`stg_green_tripdata.sql` will look like this below after:

```sql
{{ config(materialized='view') }}

select
    -- identifiers
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge
from {{source ('staging', 'green_tripdata')}}
limit 100
```


Then lets run it again:

`dbt run --select stg_green_tripdata`



If we go to the `target` folder in dbt, we can see our previous runs.


`target` -> `compiled` -> `taxi_rides_ny` -> `models` -> `staging` > `stg_green_tripdata.sql`

We can then open up `stg_green_tripdata.sql` below and see how the **macro translation** worked and added the *case statements*:


```sql
select
    -- identifiers
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge
from `new-try-zoom-data`.`trips_data_all`.`green_tripdata`
limit 100
```


### Packages

* Extra libraries that we can use

Add `packages.yml` to our project folder.

```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
```

How do we run Packages??? We run:

`dbt deps`

Which will download the dependency needed. (In this case only one) After we run `dbt deps` a new folder in our dbt project is created called `dbt_packages`


We can edit our `stg_green_tripdata.sql` file again and add the packages:

```sql
{{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid
```

`stg_green_tripdata.sql` will look like:

```sql
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge
from `new-try-zoom-data`.`trips_data_all`.`green_tripdata`
limit 100
```

After we run this `dbt run --select stg_green_tripdata` we can go to the `target` folder and see how it was translated:

```sql
select
    -- identifiers
    to_hex(md5(cast(coalesce(cast(vendorid as 
    string
), '') || '-' || coalesce(cast(lpep_pickup_datetime as 
    string
), '') as 
    string
))) as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge
from `new-try-zoom-data`.`trips_data_all`.`green_tripdata`
limit 100
```


### Now lets add variables


* Add variables to our model `stg_green_tripdata.sql`


```sql
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```

So `stg_green_tripdata.sql` will look like:

```sql
{{ config(materialized='view') }}


select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge
from {{source ('staging', 'green_tripdata')}}

where vendorid is not null
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```

Then after we run, we can check the `target` file and see it translate our variable for us.


Now were done with the **Green Trip Data**

### Yellow Trip Data

* We can do similar things to the Yellow Trip Data


```sql
{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
    row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
  from {{ source('staging','yellow_tripdata') }}
  where vendorid is not null 
)
select
   -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    cast(congestion_surcharge as numeric) as congestion_surcharge
from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```


We can run both of our models (Green and Yellow) with `dbt run` and then go to Google BigQuery and see both of our models under `dbt_dpowers` which are `stg_green_tripdata` and `stg_yellow_tripdata`.



### dbt Seeds

Meant to be used for smaller files.

You have to add the `.csv` to Github Repo or you can create the file `seeds/taxi_zone_lookup.csv` and then copy and paste into the `.csv` file.

After we uploaded the file we can run:

`dbt seed`

Then we can go to:

Go to `dbt_project.yml` and add:

```yml
seeds:
  taxi_rides_ny:
    taxi_zone_lookup:
      +column_types:
        locationid: numeric

```
This should create a table when we attempt to run `dbt seed` again.


Now that we have this seed , we can see how we can create a model

Go to the `core` folder and create a file called `dim_zones.sql`

```sql
{{ config(materialized='table') }}


select 
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone
from {{ ref('taxi_zone_lookup') }}
```

**Note:** the Core (models) will be exposed to our Business Intelligence Tools


Then we can create `fact_trips.sql` Model under `core`:

```sql
{{ config(materialized='table') }}

with green_data as (
    select *, 
        'Green' as service_type 
    from {{ ref('stg_green_tripdata') }}
), 

yellow_data as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 

trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    trips_unioned.tripid, 
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
    trips_unioned.payment_type_description, 
    trips_unioned.congestion_surcharge
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
```


[ insert linage file here!!!!!!!!!!!!!!!]

Run `dbt run` to check if everything ran [x] Note `dbt run` will only run the Models and not the *seed*

## dbt Tests

Do Later

## Deployment of a dbt Project

Help with Development Workflow

Do Later

### Google Visualize


Google  Data Studio is an online tool for converting data into *reports* and *dashboards*





