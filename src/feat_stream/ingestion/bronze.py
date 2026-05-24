import polars as pl

from feat_stream.config import settings
from feat_stream.db.postgres import connect
from feat_stream.state import bookmarks
from feat_stream.storage import s3


SOURCE_TABLES = {
    'user': 'created_on',
    'loan': 'updated_on',
    'payment': 'created_on',
}


def _distinct_dates(table, after):
    col = SOURCE_TABLES[table]
    sql = f"SELECT DISTINCT {col} AS d FROM {table} WHERE {col} > %s ORDER BY d"
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (after,))
            return [r[0] for r in cur.fetchall()]


def _read_day(table, day):
    col = SOURCE_TABLES[table]
    sql = f"SELECT * FROM {table} WHERE {col} = %s"
    with connect() as conn:
        return pl.read_database(
            query=sql, connection=conn,
            execute_options={'parameters': [day]},
        )


def ingest_table(table):
    bookmark = bookmarks.read(table)
    days = _distinct_dates(table, bookmark)
    if not days:
        return 0
    fs = s3.fs()
    total = 0
    for day in days:
        df = _read_day(table, day)
        if df.is_empty():
            continue
        key = f'{settings.s3_bucket_bronze}/{table}/batch_date={day.isoformat()}/data.parquet'
        with fs.open_output_stream(key) as out:
            df.write_parquet(out)
        bookmarks.update(table, day)
        total += df.height
    return total


def ingest_all():
    return {t: ingest_table(t) for t in SOURCE_TABLES}
