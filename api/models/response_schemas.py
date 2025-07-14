from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class CampaignPerformanceResponse(BaseModel):
    campaign_id: int
    clicks: int
    impressions: int
    ctr: float
    spend: float

class AdvertiserSpendingResponse(BaseModel):
    advertiser_id: int
    total_spend: Optional[float] = 0.0

class UserEngagement(BaseModel):
    campaign_id: int
    was_clicked: bool
    event_time: datetime

class UserEngagementsResponse(BaseModel):
    user_id: int
    engagements: List[UserEngagement]