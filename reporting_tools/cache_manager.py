import os
import json
import pandas as pd
from typing import Any, Optional, Union
from datetime import timedelta
from config.redis_config import redis_config
from upstash_redis import Redis

# Initialize Upstash Redis client
redis_url = os.getenv('UPSTASH_REDIS_URL')
redis_token = os.getenv('UPSTASH_REDIS_TOKEN')
# Convert Redis URL to HTTP URL format for Upstash
http_url = redis_url.replace('redis://', 'https://')
redis_client = Redis(url=http_url, token=redis_token)

class CacheManager:
    def __init__(self):
        """Initialize Upstash Redis client."""
        self.client = redis_client
        
    def set(self, key: str, value: Any, expire_seconds: Optional[int] = None) -> bool:
        """Set a value in Upstash Redis with optional expiration."""
        try:
            if isinstance(value, pd.DataFrame):
                # Convert DataFrame to JSON-compatible format
                value = json.dumps({'__type__': 'dataframe', 'data': value.to_json(orient='split')})
            elif isinstance(value, pd.Series):
                # Convert Series to JSON-compatible format
                value = json.dumps({'__type__': 'series', 'data': value.to_json(orient='split')})
            else:
                # Convert other types to JSON
                value = json.dumps({'__type__': 'object', 'data': value})
            
            if expire_seconds:
                self.client.set(key, value, ex=expire_seconds)
            else:
                self.client.set(key, value)
            return True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False
    
    def get(self, key: str, as_dataframe: bool = False) -> Optional[Union[pd.DataFrame, Any]]:
        """Get a value from Upstash Redis."""
        try:
            value = self.client.get(key)
            if value:
                try:
                    obj = json.loads(value)
                    if obj['__type__'] == 'dataframe':
                        return pd.read_json(obj['data'], orient='split')
                    elif obj['__type__'] == 'series':
                        return pd.read_json(obj['data'], orient='split', typ='series')
                    else:
                        return obj['data']
                except json.JSONDecodeError:
                    return value.decode('utf-8')
            return None
        except Exception as e:
            print(f"Cache get error: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """Delete a key from Upstash Redis."""
        try:
            return self.client.delete(key) > 0
        except Exception as e:
            print(f"Cache delete error: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in Upstash Redis."""
        try:
            return self.client.exists(key) > 0
        except Exception as e:
            print(f"Cache exists error: {e}")
            return False

# Singleton instance
cache = CacheManager()

# Helper functions
def cache_dataframe(df: pd.DataFrame, key: str, expire_seconds: int = 3600) -> bool:
    """Cache a DataFrame with expiration in Upstash Redis."""
    return cache.set(key, df, expire_seconds)

def get_cached_dataframe(key: str) -> Optional[pd.DataFrame]:
    """Get a cached DataFrame from Upstash Redis."""
    return cache.get(key, as_dataframe=True)

def invalidate_cache(key: str) -> bool:
    """Invalidate a cache entry in Upstash Redis."""
    return cache.delete(key)
