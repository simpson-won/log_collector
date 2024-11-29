"""
log_trace.py

"""
import os
import time
from logging import Logger

from lib.mongo_logs import celery_task as vms_celery_task
from lib.aws_logs import celery_task as aws_celery_task


# from lib.mongo_logs import data_parse_process

LINE_CMD = '\"c\":\"COMMAND\"'
LINE_AUTH = '\"c\":\"ACCESS\"'
LINE_EX_MSG1 = 'Slow'
LINE_AUTH_DETAIL1 = "Authentication succeeded"
LINE_AUTH_DETAIL2 = "Successfully authenticated"

target_tasks = {"vms": vms_celery_task, "aws": aws_celery_task}


"""
trace_log

:log_fd
:logger
"""


def trace_log(log_fd, logger: Logger, op_version: int = 2, target: str = "vms"):
    """trace_log"""
    logger.info('start follow')
    log_fd.seek(0, 2)
    from log_collector_v3 import is_log_trace, set_retry_count, set_retry_this
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
                if LINE_CMD in line:
                    if LINE_EX_MSG1 not in line and 'Applying default' not in line:
                        target_tasks[target].delay(line)
                elif LINE_AUTH in line:
                    if LINE_AUTH_DETAIL1 in line or LINE_AUTH_DETAIL2 in line:
                        target_tasks[target].delay(line)
        except FileNotFoundError as file_not_found:
            logger.info('trace_log: FileNotFoundError\n\t\t%s', str(file_not_found))
            set_retry_this(True)
            return False
        except Exception as general_except:
            logger.info('trace_log: Exception\n\t\t%s', str(general_except))
            set_retry_this(False)
            return False
    logger.info('end follow')
    return True
