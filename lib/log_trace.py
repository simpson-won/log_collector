import os
from datetime import time
import json
from logging import Logger

from lib.mongo_logs import check_authenticated, check_command, check_accept_state, check_connection_ended, check_returning_user_from_cache
from service import redis_client, send_to_redis


monitoring_lines = {"Connection accepted": check_accept_state,
                    "Returning user from cache": check_returning_user_from_cache,
                    "About to run the command": check_command,
                    "Successfully authenticated": check_authenticated,
                    "Connection ended": check_connection_ended,
                    }


def data_parse_process(data):
    if data.startswith('{'):
        log_dict = json.loads(data)
        if log_dict["msg"] in monitoring_lines.keys():
            monitoring_lines.get(log_dict["msg"])(log_dict)


def trace_log(log_fd, logger: Logger):
    logger.info(f'start follow - {log_fd}')
    log_fd.seek(0, 2)
    from log_collector_v2 import is_log_trace, set_retry_count, set_retry_this
    set_retry_this(False)
    not_read_cnt = 0
    while is_log_trace:
        try:
            os.stat(log_fd.name)
            line = log_fd.readline()
            if not line:
                if not_read_cnt > 600:
                    set_retry_count(True)
                    set_retry_this(True)
                    return False
                not_read_cnt += 1
                time.sleep(0.1)
            else:
                not_read_cnt = 0
                set_retry_this(False)
                #data_parse_process(data=line)
                send_to_redis(logger, redis_client, line)
        except FileNotFoundError as file_not_found:
            logger.info(f'trace_log: FileNotFoundError\n\t\t{file_not_found}')
            set_retry_this(True)
            return False
        except Exception as general_except:
            logger.info(f'trace_log: Exception\n\t\t{general_except}')
            set_retry_this(False)
            return False
    logger.info(f'end follow - {log_fd}')
    return True
