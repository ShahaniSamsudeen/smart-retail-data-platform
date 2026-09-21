{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_sales AS (

    SELECT
        sales_key,
        transaction_id,
        date_key,
        product_key,
        customer_key,
        store_key,
        quantity,
        unit_price,
        net_sales,
        payment_method,
        sales_channel

    FROM {{ source('retaillake_gold', 'fact_sales_transaction') }}

),

cleaned_sales AS (

    SELECT
        sales_key,
        TRIM(transaction_id) AS transaction_id,
        date_key,
        product_key,
        customer_key,
        store_key,
        CAST(quantity AS NUMERIC(12, 2)) AS quantity,
        CAST(unit_price AS NUMERIC(12, 2)) AS unit_price,
        CAST(net_sales AS NUMERIC(14, 2)) AS net_sales,
        LOWER(TRIM(payment_method)) AS payment_method,
        LOWER(TRIM(sales_channel)) AS sales_channel

    FROM source_sales

)

SELECT *
FROM cleaned_sales