from pathlib import Path

path = Path("scripts/gold_fact_sales.py")
content = path.read_text()

content = content.replace(
'''gold_df.to_sql(
    "fact_sales_transaction",
    engine,
    schema="gold",
    if_exists="replace",
    index=False,
    method="multi"
)''',
'''with engine.begin() as connection:
    connection.execute(
        text("DELETE FROM gold.fact_sales_transaction")
    )

gold_df.to_sql(
    "fact_sales_transaction",
    engine,
    schema="gold",
    if_exists="append",
    index=False,
    method="multi"
)'''
)

path.write_text(content)
print("gold_fact_sales.py updated successfully.")
