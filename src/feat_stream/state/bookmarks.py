from datetime import date
from feat_stream.db.postgres import connect

DEFAULT_BOOKMARK = date.min
def read(table_name):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_processed_date FROM pipeline_bookmarks WHERE table_name = %s",
                (table_name,),
            )
            row = cur.fetchone()
            return row[0] if row else DEFAULT_BOOKMARK

def update(table_name, new_date):
    sql = """
        INSERT INTO pipeline_bookmarks (table_name, last_processed_date, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (table_name) DO UPDATE
        SET last_processed_date = EXCLUDED.last_processed_date,
            updated_at = NOW()
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (table_name, new_date))
        conn.commit()
