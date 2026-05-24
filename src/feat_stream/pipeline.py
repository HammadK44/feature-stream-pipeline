import time
from feat_stream.config import settings
from feat_stream.features.compute import build_gold
from feat_stream.ingestion.bronze import ingest_all
from feat_stream.ingestion.silver import build_loan_payments

def run_once():
    bronze = ingest_all()
    silver = build_loan_payments()
    gold = build_gold()
    return {'bronze': bronze, 'silver': silver, 'gold': gold}

def run_forever():
    while True:
        try:
            result = run_once()
            print(result)
        except Exception as e:
            print(f'cycle failed: {e}')
        time.sleep(settings.poll_interval_seconds)
