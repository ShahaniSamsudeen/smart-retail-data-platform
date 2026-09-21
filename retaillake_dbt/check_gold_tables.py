from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://retaillake_user:change_me@localhost:5432/retaillake"

engine = create_engine(DATABASE_URL)

with engine.connect() as connection:
    query = text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
        ORDER BY table_name;
    """)

    results = connection.execute(query)

    print("\nTables in the gold schema:\n")

    for row in results:
        print(f"{row[0]}.{row[1]}")