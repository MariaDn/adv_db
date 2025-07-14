import time
from database import MySQLDatabase
from cache import RedisCache
import json
from datetime import datetime
from models.response_schemas import UserEngagementsResponse

class UserService:
    def __init__(self, db: MySQLDatabase, cache: RedisCache, ttl=60):
        self.db = db
        self.cache = cache
        self.ttl = ttl

    def get_user_engagements(self, user_id: int) -> UserEngagementsResponse:
        cache_key = f"user:{user_id}:engagements"
        start = time.perf_counter()

        cached = self.cache.get(cache_key)
        if cached:
            duration = time.perf_counter() - start
            print(f"[CACHE HIT] {cache_key} → took {duration:.4f}s")
            return UserEngagementsResponse.parse_raw(cached)

        print(f"[CACHE MISS] {cache_key} → querying DB...")
        engagements = self.db.fetch_user_engagements(user_id)
        duration = time.perf_counter() - start
        print(f"[DB QUERY] took {duration:.4f}s")

        if not engagements:
            return None

        for e in engagements:
            if isinstance(e.get("event_time"), datetime):
                e["event_time"] = e["event_time"].isoformat()

        result = {"user_id": user_id, "engagements": engagements}
        self.cache.set(cache_key, json.dumps(result), self.ttl)
        return UserEngagementsResponse(**result)