import pickle
import time
from typing import Any, Dict, Optional
import redis.asyncio as redis

class CacheManager:
    """Cache service for API responses"""
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.local_cache: Dict[str, tuple] = {}  # (data, expiry)
        self.local_ttl = 10  # Local cache TTL in seconds

    async def get(self, key: str) -> Optional[Any]:
        # Check local cache first for better performance
        now = time.time()
        if key in self.local_cache and self.local_cache[key][1] > now:
            return self.local_cache[key][0]

        # Fall back to Redis
        cached_data = await self.redis.get(key)
        if cached_data:
            data = pickle.loads(cached_data)
            # Update local cache
            self.local_cache[key] = (data, now + self.local_ttl)
            return data
        return None

    async def set(self, key: str, value: Any, ttl: int) -> None:
        serialized_data = pickle.dumps(value)
        # Set in Redis
        await self.redis.setex(key, ttl, serialized_data)
        # Update local cache
        self.local_cache[key] = (value, time.time() + min(ttl, self.local_ttl))

    async def invalidate(self, pattern: str) -> None:
        # Clear local cache entries that match the pattern
        keys_to_remove = [k for k in self.local_cache if pattern in k]
        for k in keys_to_remove:
            del self.local_cache[k]
        
        # Clear Redis cache
        keys = await self.redis.keys(f"*{pattern}*")
        if keys:
            await self.redis.delete(*keys)
