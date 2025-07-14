import redis
import json
from config import settings

class RedisCache:
    def __init__(self):
        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True
        )

    def get(self, key: str):
        data = self.client.get(key)
        if data:
            return json.loads(data)
        return None

    def set(self, key: str, value: dict, ttl: int):
        self.client.setex(key, ttl, json.dumps(value))