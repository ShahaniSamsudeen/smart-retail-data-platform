{{ config(
    materialized='table',
    schema='gold'
) }}

WITH sales_data AS (

    SELECT
        date_key,
        product_key,
        store_key,
        sales_channel,
        payment_method,
        quantity,
        unit_price,
        net_sales

    FROM {{ ref('silver_sales_transaction') }}

)

SELECT
    date_key,
    product_key,
    store_key,
    sales_channel,
    payment_method,

    COUNT(*) AS total_transactions,

    SUM(quantity) AS total_quantity_sold,

    SUM(net_sales) AS total_sales_revenue,

    AVG(unit_price) AS average_unit_price

FROM sales_data

GROUP BY
    date_key,
    product_key,
    store_key,
    sales_channel,
    payment_method