import polars as pl

from feat_stream.config import settings
from feat_stream.storage import s3


LOAN_RENAMES = {
    'id': 'loan_id',
    'amount': 'loan_amount',
    'status': 'loan_status',
    'created_on': 'loan_created_on',
    'updated_on': 'loan_updated_on',
    'matured_on': 'loan_matured_on',
}

PAYMENT_RENAMES = {
    'id': 'payment_id',
    'amount': 'payment_amount',
    'status': 'payment_status',
    'created_on': 'payment_created_on',
}


def _read_bronze_table(fs, table):
    keys = s3.list_parquet_keys(settings.s3_bucket_bronze, f'{table}/')
    frames = []
    for key in keys:
        with fs.open_input_file(f'{settings.s3_bucket_bronze}/{key}') as f:
            frames.append(pl.read_parquet(f))
    # added diagonal_relaxed becaus lets tolerate per-file schema drift because some days had all NULL matured_on, which pyarrow inferred as null-typed
    return pl.concat(frames, how='diagonal_relaxed')


def build_loan_payments():
    fs = s3.fs()
    loans = _read_bronze_table(fs, 'loan').rename(LOAN_RENAMES)
    payments = _read_bronze_table(fs, 'payment').rename(PAYMENT_RENAMES)
    joined = loans.join(payments, on='loan_id', how='left')
    key = f'{settings.s3_bucket_silver}/loan_payments/data.parquet'
    with fs.open_output_stream(key) as out:
        joined.write_parquet(out)
    return joined.height
