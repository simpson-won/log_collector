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


def queue_push(logger: Logger, handle: redis.Redis, data: str):
    try:
        handle.lpush(redis_channel, data)
    except Exception as e:
        logger.error(f"queuing: Exception\n\t\t{e}")


def queue_pop(logger: Logger, handle: redis.Redis, process):
    try:
        while True:
            res = handle.rpop(redis_channel)
            if res is not None:
                logger.info(f'queue_pop: res = {res}')
                process(data=res)
            else:
                time.sleep(0.01)
    except KeyboardInterrupt:
        logger.info("recv_from_redis: User are requested to stop.")
        handle.close()
        return
    except Exception as e:
        logger.info(f"recv_from_redis: Exception\n\t\t{e}")
    handle.close()


def recv_from_redis(logger: Logger, handle: redis.Redis, process):
    pubsub = handle.pubsub()
    pubsub.subscribe(config.redis_channel)
    try:
        while True:
            res = pubsub.get_message()
            # logger.info(f"recv_from_redis: res={res}")
            if res is not None:
                process(data=res['data'])
            else:
                time.sleep(0.01)
    except KeyboardInterrupt:
        logger.info("recv_from_redis: User are requested to stop.")
        handle.close()
        return
    except Exception as e:
        logger.info(f"recv_from_redis: Exception\n\t\t{e}")
    handle.close()
