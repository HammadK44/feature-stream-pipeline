from contextlib import asynccontextmanager
import polars as pl
from fastapi import FastAPI
from feat_stream.config import settings
from feat_stream.features.schema import ClientFeatures
from feat_stream.storage import s3

_state = {'features': {}, 'loaded_at': None}

def _load():
    fs = s3.fs()
    key = f'{settings.s3_bucket_gold}/client_features/data.parquet'
    with fs.open_input_file(key) as f:
        df = pl.read_parquet(f)
    return {r['client_id']: ClientFeatures(**r) for r in df.to_dicts()}

def _swap(fresh):
    _state['features'] = fresh
    _state['loaded_at'] = max(f.computed_at for f in fresh.values())

@asynccontextmanager
async def lifespan(app):
    _swap(_load())
    yield

app = FastAPI(lifespan=lifespan)

@app.get('/health')
def health():
    return {
        'status': 'ok',
        'client_count': len(_state['features']),
        'loaded_at': _state['loaded_at'],
    }

@app.get('/features/{client_id}', response_model=ClientFeatures)
def get_features(client_id: int):
    return _state['features'].get(client_id)
