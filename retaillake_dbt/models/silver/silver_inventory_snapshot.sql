{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_inventory AS (

    SELECT
        inventory_key,
        date_key,
        product_key,
        store_key,
        stock_quantity

    FROM {{ source('retaillake_gold', 'fact_inventory_snapshot') }}

),

cleaned_inventory AS (

    SELECT
        inventory_key,
        date_key,
        product_key,
        store_key,
        CAST(stock_quantity AS NUMERIC(12, 2)) AS stock_quantity

    FROM source_inventory

)

SELECT *
FROM cleaned_inventory