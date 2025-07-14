import time
from cache import RedisCache
from database import MySQLDatabase
import json

class CampaignService:
    def __init__(self, db: MySQLDatabase, cache: RedisCache, ttl=30):
        self.db = db
        self.cache = cache
        self.ttl = ttl

    def get_campaign_performance(self, campaign_id: int):
        cache_key = f"campaign:{campaign_id}:performance"
        start_redis = time.perf_counter()
        cached = self.cache.get(cache_key)
        if cached:
            duration = time.perf_counter() - start_redis
            print(f"[CACHE HIT] {cache_key} → took {duration:.4f}s")
            return json.loads(cached)

        print(f"[CACHE MISS] {cache_key} → querying DB...")
        start_db = time.perf_counter()
        row = self.db.fetch_campaign_performance(campaign_id)
        duration = time.perf_counter() - start_db
        print(f"[DB QUERY] took {duration:.4f}s")

        if not row:
            return None

        data = {
            "campaign_id": campaign_id,
            "clicks": int(row.get("clicks", 0)),
            "impressions": int(row.get("impressions", 0)),
            "ctr": float(row.get("ctr", 0.0)),
            "spend": float(row.get("spend", 0.0)),
        }

        self.cache.set(cache_key, json.dumps(data), self.ttl)
        return data