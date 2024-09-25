import os
import time
import json
from logging import Logger

from service import redis_client, send_to_redis


# from lib.mongo_logs import data_parse_process

line_cmd = '\"c\":\"COMMAND\"'
line_auth = '\"c\":\"ACCESS\"'
line_ex_msg1 = 'Slow'


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
                # data_parse_process(data=line)
                if (line_cmd  in line) or (line_auth in line):
                    if line_ex_msg1 not in line and 'Applying default' not in line:
                        send_to_redis(logger, redis_client, line)
                #    if log_dict["msg"] in monitoring_lines.keys():
                #        monitoring_lines[log_dict["msg"]](log_dict)
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

