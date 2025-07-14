from fastapi import FastAPI, HTTPException
from database import MySQLDatabase
from cache import RedisCache
from services.campaign_service import CampaignService
from services.advertiser_service import AdvertiserService
from services.user_service import UserService
from models.response_schemas import (
    CampaignPerformanceResponse,
    AdvertiserSpendingResponse,
    UserEngagementsResponse
)

app = FastAPI(title="Ad Analytics API with Redis Cache")

# Ініціалізація сервісів
db = MySQLDatabase()
cache = RedisCache()

campaign_service = CampaignService(db, cache)
advertiser_service = AdvertiserService(db, cache)
user_service = UserService(db, cache)

@app.get("/campaign/{campaign_id}/performance", response_model=CampaignPerformanceResponse)
def get_campaign_performance(campaign_id: int):
    data = campaign_service.get_campaign_performance(campaign_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return data

@app.get("/advertiser/{advertiser_id}/spending", response_model=AdvertiserSpendingResponse)
def get_advertiser_spending(advertiser_id: int):
    data = advertiser_service.get_advertiser_spending(advertiser_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Advertiser not found")
    return data

@app.get("/user/{user_id}/engagements", response_model=UserEngagementsResponse)
def get_user_engagements(user_id: int):
    data = user_service.get_user_engagements(user_id)
    if data is None:
        raise HTTPException(status_code=404, detail="User not found or no engagements")
    return data