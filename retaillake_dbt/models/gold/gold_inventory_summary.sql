{{ config(
    materialized='table',
    schema='gold'
) }}

WITH inventory_data AS (

    SELECT
        date_key,
        product_key,
        store_key,
        stock_quantity

    FROM {{ ref('silver_inventory_snapshot') }}

)

SELECT
    date_key,
    product_key,
    store_key,

    COUNT(*) AS inventory_record_count,

    SUM(stock_quantity) AS total_stock_quantity,

    AVG(stock_quantity) AS average_stock_quantity

FROM inventory_data

GROUP BY
    date_key,
    product_key,
    store_key