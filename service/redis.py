import redis
import time
from logging import Logger

import config
from config import redis_host, redis_channel, redis_port, redis_db


def redis_init():
    handle = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    
    return handle


def send_to_redis(logger: Logger, handle: redis.Redis, data: str):
    try:
        handle.publish(channel=redis_channel, message=data)
    except Exception as e:
        logger.error(f"send_to_redis: Exception\n\t\t{e}")


def recv_from_redis(logger: Logger, handle: redis.Redis, process):
    try:
        pubsub = handle.pubsub()
        pubsub.subscribe(config.redis_channel)

        while True:
            res = pubsub.get_message()
            if res is not None:
                print(res)
                process(data=res)
            else:
                time.sleep(0.1)
    except Exception as e:
        logger.info(f"recv_from_redis: Exception\n\t\t{e}")
