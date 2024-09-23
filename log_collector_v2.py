import time
import signal
from lib.signal import sig_init
from lib.pid import write_pid
from log import logger
from lib.log_trace import trace_log
from service import recv_from_redis, redis_client
from lib.args import get_args

run_mode = 'publisher'

retry_this = True
is_log_trace = True


retry_count = 0


def set_retry_this(value: bool):
    global retry_this
    retry_this = value


def set_retry_count(value: int):
    global retry_count
    retry_count = value


def log_monitor(file_name: str):
    global retry_this
    global retry_count
    global is_log_trace
    try:
        with open(file_name, "rt") as fd:
            logger.info(f'log_monitor: success to open file {fd}')
            is_log_trace = True
            result = trace_log(fd, logger=logger)
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


def stop_func(sig_num, data):
    global is_log_trace
    global retry_this
    global retry_count
    
    logger.info(f'stop_func: by sigusr1 [sig_num = {sig_num}, data={data}')
    is_log_trace = False
    retry_count = 0
    retry_this = True


if __name__ == "__main__":
    mongodb_log_path, run_mode = get_args()
    
    write_pid(file_path="log_collector.pid")
    sig_init(signals=[(signal.SIGUSR1, stop_func)])
    logger.info(f'main: log_path={mongodb_log_path}, run_mode={run_mode}')
    if run_mode == "publisher":
        retry_run(log_path=mongodb_log_path)
    else:
        recv_from_redis(redis_client)
    
