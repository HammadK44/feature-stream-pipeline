from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=(".env", ".env.local"), extra="ignore")
    postgres_host: str
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_password: str
    s3_endpoint_url: str
    s3_region: str = "us-east-1"
    s3_bucket_bronze: str
    s3_bucket_silver: str
    s3_bucket_gold: str
    s3_access_key: str
    s3_secret_key: str
    poll_interval_seconds: int = 30

settings = Settings()