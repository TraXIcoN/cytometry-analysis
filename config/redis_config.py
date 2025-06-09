import os
from dotenv import load_dotenv

class RedisConfig:
    """Configuration for Upstash Redis caching."""
    
    def __init__(self):
        load_dotenv()
        self.url = os.getenv('UPSTASH_REDIS_URL')
        if not self.url:
            raise ValueError("UPSTASH_REDIS_URL environment variable is not set")
        
    def get_connection_pool(self):
        """Get Redis connection pool for Upstash."""
        import redis
        from urllib.parse import urlparse
        
        parsed_url = urlparse(self.url)
        return redis.ConnectionPool(
            host=parsed_url.hostname,
            port=parsed_url.port,
            password=parsed_url.password,
            max_connections=100,
            socket_timeout=5,
            socket_connect_timeout=5
        )

# Singleton instance
redis_config = RedisConfig()

# Singleton instance
redis_config = RedisConfig()
