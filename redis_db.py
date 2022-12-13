import redis
import settings

def get_redis_db(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB):
    # source https://github.com/redis/redis-py
    return redis.Redis(host=host, port=port, db=db)