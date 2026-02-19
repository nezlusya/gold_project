import pandas as pd
import psycopg2


def load_gold_6m():
    conn = psycopg2.connect(
        host="postgres_dwh",
        port=5432,
        user="gold_user",
        password="gold_pass",
        database="gold_db"
    )

    query = """
        SELECT
            date,
            buy_price,
            sell_price
        FROM ods.gold_price_cbr_6
        ORDER BY date;
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df
