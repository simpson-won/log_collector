import time
import os
import argparse
import json
import sys
import signal
import redis
from config import redis_db, redis_host, redis_port, redis_channel
from service.redis import reev_from_redis, send_to_redis
from lib.mongo_logs import check_authenticated, check_command, check_accept_state, check_connection_ended, check_returning_user_from_cache

from log import logger

retry_this = True
is_log_trace = True


retry_count = 0


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


def trace_log(log_fd):
    global retry_this
    global retry_count
    
    logger.info(f'start follow - {log_fd}')
    log_fd.seek(0, 2)
    retry_this = False
    not_read_cnt = 0
    while is_log_trace:
        try:
            os.stat(log_fd.name)
            line = log_fd.readline()
            if not line:
                if not_read_cnt > 600:
                    retry_this = True
                    return False
                not_read_cnt += 1
                time.sleep(0.1)
                continue
            else:
                not_read_cnt = 0
                retry_this = False
                data_parse_process(data=line)
        except FileNotFoundError as file_not_found:
            logger.info('trace_log: FileNotFoundError\n\t\t{file_not_found}')
            retry_this = True
            return False
        except Exception as general_except:
            logger.info('trace_log: Exception\n\t\t{file_not_found}')
            retry_this = False
            return False
    logger.info(f'end follow - {log_fd}')
    return True


def get_arg_logpath():
    parser = argparse.ArgumentParser(description='Mongodb Log Monitoring.')
    parser.add_argument('--filepath', dest='filepath', action='store', default="")
    args = parser.parse_args()
    return args.filepath


def log_monitor(file_name: str):
    global retry_this
    global retry_count
    global is_log_trace
    try:
        with open(file_name, "rt") as fd:
            logger.info(f'log_monitor: success to open file {fd}')
            is_log_trace = True
            result = trace_log(fd)
            fd.close()
            return result
    except FileNotFoundError as file_not_found:
        logger.info(f'log_monitor: FileNotFoundError\n\t\t{file_not_found}')
        retry_this = True
        return False
    retry_this = False
    return False


def retry_run(log_path):
    global retry_count
    global retry_this
    
    while retry_this:
        log_monitor(file_name=log_path)
        logger.info(f'retry_run: retry_count={retry_count} retry_this={retry_this}')
        if retry_count < 600:
            retry_count += 1
            time.sleep(1)
        else:
            logger.info('retry_run: exit')
            break


def sig_handler(sig_num, frame):
    global is_log_trace
    global retry_this
    global retry_count
    
    if sig_num == signal.SIGUSR1:
        logger.info('sig_handler: reload_log_trace')
        is_log_trace = False
        retry_count = 0
        retry_this = True
    if sig_num in [signal.SIGKILL, signal.SIGTERM, signal.SIGSEGV, signal.SIGHUP, signal.SIGABRT]:
        logger.info(f'sig_handler: {sig_num}')
        sys.exit()


def write_pid():
    with open("log_collector.pid", "wt") as fd:
        fd.write(f"{os.getpid()}")
        fd.close()


if __name__ == "__main__":
    try:
        mongodb_log_path = os.environ['MONGO_LOG']
    except Exception as e:
        mongodb_log_path = get_arg_logpath()
    
    write_pid()
    
    signal.signal(signal.SIGUSR1, sig_handler)
    
    logger.info(f'main: log_path={mongodb_log_path}')
    retry_run(log_path=mongodb_log_path)
