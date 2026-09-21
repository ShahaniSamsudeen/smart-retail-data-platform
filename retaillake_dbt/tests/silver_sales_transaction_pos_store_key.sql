SELECT
    sales_key,
    transaction_id,
    store_key,
    sales_channel

FROM {{ ref('silver_sales_transaction') }}

WHERE sales_channel = 'pos'
  AND store_key IS NULL