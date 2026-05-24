from datetime import datetime, date, timedelta
import polars as pl
from feat_stream.config import settings
from feat_stream.storage import s3

FUNDED_STATUSES = ['active', 'paid', 'overdue']
PROFIT_WINDOW_DAYS = 90

def _read_silver(fs):
    key = f'{settings.s3_bucket_silver}/loan_payments/data.parquet'
    with fs.open_input_file(key) as f:
        return pl.read_parquet(f)

def _read_clients(fs):
    keys = s3.list_parquet_keys(settings.s3_bucket_bronze, 'user/')
    frames = []
    for k in keys:
        with fs.open_input_file(f'{settings.s3_bucket_bronze}/{k}') as f:
            frames.append(pl.read_parquet(f).select('id'))
    return pl.concat(frames, how='diagonal_relaxed').rename({'id': 'client_id'}).unique()

def paid_loans_count(silver):
    return (silver.filter(pl.col('loan_status') == 'paid')
        .group_by('loan_id', 'client_id').agg().group_by('client_id').len()
        .rename({'len': 'paid_loans_count'}))

def days_since_last_late_payment(silver, today):
    return (silver.filter(pl.col('payment_status') == 'late')
        .group_by('client_id')
        .agg(pl.col('payment_created_on').max().alias('last_late'))
        .with_columns((pl.lit(today) - pl.col('last_late')).dt.total_days().alias('days_since_last_late_payment'))
        .select('client_id', 'days_since_last_late_payment'))

def profit_in_last_90_days_rate(silver, today):
    window_start = today - timedelta(days=PROFIT_WINDOW_DAYS)
    recent = silver.filter((pl.col('loan_created_on') >= window_start) & (pl.col('loan_status').is_in(FUNDED_STATUSES)))
    interest_per_loan = (recent.group_by('loan_id').agg(pl.col('interest').sum().alias('loan_interest')))
    loans_per_client = (recent.group_by('loan_id', 'client_id', 'loan_amount').agg()
        .join(interest_per_loan, on='loan_id', how='left')
        .with_columns(pl.col('loan_interest').fill_null(0)))
    return (loans_per_client.group_by('client_id')
        .agg(pl.col('loan_amount').sum().alias('sum_amount'), pl.col('loan_interest').sum().alias('sum_interest'))
        .with_columns((pl.col('sum_interest') / pl.col('sum_amount')).alias('profit_in_last_90_days_rate'))
        .select('client_id', 'profit_in_last_90_days_rate'))

def build_gold():
    fs = s3.fs()
    silver = _read_silver(fs)
    clients = _read_clients(fs)
    today = date.today()
    f1 = paid_loans_count(silver)
    f2 = days_since_last_late_payment(silver, today)
    f3 = profit_in_last_90_days_rate(silver, today)
    gold = (clients.join(f1, on='client_id', how='left').join(f2, on='client_id', how='left').join(f3, on='client_id', how='left')
        .with_columns(pl.col('paid_loans_count').fill_null(0)).with_columns(pl.lit(datetime.now()).alias('computed_at')))
    key = f'{settings.s3_bucket_gold}/client_features/data.parquet'
    with fs.open_output_stream(key) as out:
        gold.write_parquet(out)
    return gold.height
