import psycopg2
from feat_stream.config import settings

def connect():
    return psycopg2.connect(
        host=settings.postgres_host, port=settings.postgres_port,
        dbname=settings.postgres_db, user=settings.postgres_user,
        password=settings.postgres_password,
    )
