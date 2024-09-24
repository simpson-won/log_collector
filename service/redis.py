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
    pubsub = handle.pubsub()
    pubsub.subscribe(config.redis_channel)
    try:
        while True:
            res = pubsub.get_message()
            if res is not None:
                process(data=res['data'])
                #print(f'recv_from_redis: {res["data"]}')
            else:
                time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("recv_from_redis: User are requested to stop.")
        handle.close()
        return
    except Exception as e:
        logger.info(f"recv_from_redis: Exception\n\t\t{e}")
    handle.close()
