from datetime import datetime
from pydantic import BaseModel

class ClientFeatures(BaseModel):
    client_id: int
    paid_loans_count: int
    days_since_last_late_payment: int | None
    profit_in_last_90_days_rate: float | None
    computed_at: datetime
