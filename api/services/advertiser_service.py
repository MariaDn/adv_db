import time
from cache import RedisCache
from database import MySQLDatabase
import json

class AdvertiserService:
    def __init__(self, db: MySQLDatabase, cache: RedisCache, ttl=300):
        self.db = db
        self.cache = cache
        self.ttl = ttl

    def get_advertiser_spending(self, advertiser_id: int):
        cache_key = f"advertiser:{advertiser_id}:spending"
        start = time.perf_counter()
        cached = self.cache.get(cache_key)
        if cached:
            duration = time.perf_counter() - start
            print(f"[CACHE HIT] {cache_key} → took {duration:.4f}s")
            return json.loads(cached)

        print(f"[CACHE MISS] {cache_key} → querying DB...")
        start = time.perf_counter()
        row = self.db.fetch_advertiser_spending(advertiser_id)
        duration = time.perf_counter() - start
        print(f"[DB QUERY] took {duration:.4f}s")

        if not row:
            return None

        data = {
            "advertiser_id": advertiser_id,
            "total_spend": float(row.get("total_spend", 0.0))
        }

        self.cache.set(cache_key, json.dumps(data), self.ttl)
        return data
